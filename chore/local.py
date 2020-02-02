#
# Copyright (C) 2017-2018 Maha Farhat
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Author: Martin Owens
"""
This is the Docker way of running jobs, via local Docker
"""

import os
import time
import pytz
from datetime import datetime
import threading
import random

import docker
from docker.types import Mount
from docker.models.containers import Container

from .base import JobManagerBase, JobSubmissionError, now, settings

import logging
logger = logging.getLogger(__name__)


class DockerJobManager(JobManagerBase):
    """
    The job is submitted to the Docker daemon and is controlled via container id.
    This container id must be stored in the pipeline-shell directory in tmp.
    """
    # Parameters needed for running jobs in Docker
    label_key = 'gentb-chore'

    @classmethod
    def image_name(cls):
        """
        Fetches the name of the Docker image to use for jobs, looks in GenTb settings
        :return: str
        """
        return getattr(settings, 'GENTB_PIPELINE_JOB_DEFINITION')

    @classmethod
    def is_test(cls):
        """
        Fetches the testing status, looks in GenTb settings
        :return: bool
        """
        return getattr(settings, 'GENTB_PIPELINE_TEST')

    @classmethod
    def _list_containers(cls, status=None):
        """
        Fetches all current job containers. If passed, returned containers
        are filtered on their current status (e.g. 'running', 'exited')
        :rtype: list
        """
        client = docker.from_env()

        # Compile the list
        job_container_ids = []

        # List running containers
        for container in client.containers.list():

            # Check labels to ensure it's a Pipeline job
            if cls.label_key in container.labels:

                # Check status if specified
                if not status or container.status == status:
                    job_container_ids.append(container.id)

        return job_container_ids

    @classmethod
    def _get_container(cls, id_or_name):
        """
        Fetches the container for the id or name, if it exists.
        :rtype: docker.Container
        """
        client = docker.from_env()
        try:
            return client.containers.get(id_or_name)
        except docker.errors.NotFound:
            logger.debug('Job/container: "{}" not found'.format(id_or_name))

        return None

    @classmethod
    def _remove_container(cls, id_or_name):
        """
        Removes the container and cleans up
        :param id_or_name: The id of the container
        :returns Whether the container was removed or not
        """
        client = docker.from_env()
        try:
            # Get the container and remove it
            container = client.containers.get(id_or_name)
            container.remove()
            return True
        except docker.errors.NotFound:
            return False

    @classmethod
    def _stop_container(cls, id):
        """
        Find the container and kills it
        :rtype: bool
        """
        container = cls._get_container(id)
        if container:
            container.kill()
            return True

        return False

    @classmethod
    def _create_container(cls, name, command, files_path=None, **kwargs):
        """
        Creates a container for running the passed command
        :param input: The location of the file to work on
        :param command: The command to run on the container
        :param files_path: The path to the files directory
        :return: The container
        :rtype: Container
        """
        logger.debug('Job: {}'.format(name))

        # Get docker client
        client = docker.from_env()

        # Get image to use from GenTb
        image_name = cls.image_name()
        logger.debug('Image: {}'.format(image_name))
        if not image_name:
            raise JobSubmissionError('Image name must be specified in "GENTB_PIPELINE_JOB_DESCRIPTION"')

        # Find the current container running Gentb and mount its data volume
        mount = None
        for container in client.containers.list():
            if mount:
                break

            # Check volumes
            for _mount in container.attrs['Mounts']:
                # Check destination
                if files_path and _mount['Destination'] in files_path:
                    volume = _mount['Source']

                    # Prepare mount
                    mount = Mount(target=_mount['Destination'], source=os.path.basename(os.path.dirname(volume)), type='volume')
                    break

        # Create labels for container
        labels = {
            cls.label_key: 'true',
            'gentb-job': name,
        }

        # Check if testing
        if cls.is_test():
            logger.debug('Job/{}: Is testing'.format(name))

            # Set a test command to merely sleep for a random time and then produce the necessary output files
            # by copying forward the input file(s)
            input_files = kwargs.get('input_files')
            output_files = kwargs.get('output_files')
            sleep = random.randint(10, 30)
            if input_files and output_files:
                command = "sh -c 'sleep {} ; {} ; exit 0'".format(
                    sleep,
                    ' ; '.join(['cp -rp {} {}'.format(input_files[0], o) for o in output_files])
                )
            else:
                command = 'sleep {}'.format(sleep)

            # Add testing to labels
            labels['gentb-test'] = 'true'

        # Build the container
        container = client.containers.create(
            name=name,
            image=image_name,
            command=command,
            labels=labels,
            detach=True,
            mounts=[mount] if mount else [],
        )

        return container

    @classmethod
    def _run_container(cls, name, command, **kwargs):
        """
        Runs a command using the set image
        :param input: The location of the file to work on
        :param command: The command to run on the container
        :param output: The location to place the output
        :return: The container
        :rtype: Container
        """
        # Check if created
        container = cls._get_container(name)
        if not container:

            # Create it
            container = cls._create_container(name, command, **kwargs)

        # Start it
        container.start()

        return container

    def job_submit(self, job_id, cmd, depend=None, **kwargs):
        """
        Run the container to execute the cmd. As jobs are added sequentially but regardless of the status of
        preceding or following jobs, we've got to maintain some state to determine when and if a job
        should actually be run. This is maintained through the state of the Docker containers for each job.
        Notes:
            1. A created container means a job is not yet started due to waiting for the preceding job to finish
            2. A running container is, obviously, a running job
            3. An exited container is either a completed job, or a failed job; exit code determines this
                i. a completed job, with exit code 0
                ii. a failed job, with exit code 1
            4. A created container pending a preceding container that disappears indicates an abandoned pipeline
                i. As soon as a container fails, it is removed, indicating to waiting containers to shut it all down
        """
        if depend:
            logger.debug('Job/{} - Depend/{}: Submitting job after dependent'.format(job_id, depend))

            # Ensure it exists
            container = self._get_container(depend)
            if not container:
                # Raise error and bail
                self._quit_pipeline(job_id, "Couldn't find dependent job: {}".format(container.status), depend)

            # Check if running or created (pending)
            elif container.status == 'running' or container.status == 'created':
                logger.debug('Job/{} - Depend/{}: Dependent status "{}"'.format(job_id, depend, container.status))

                # If we depend on another process and it's not yet finished, create this job, if
                # not already created
                container = self._get_container(job_id)
                if not container:

                    # Create it
                    container = self._create_container(job_id, cmd, **kwargs)

                    # Queue it
                    logger.debug('Job/{} ({}) - Depend/{}: Dependent waiting/running'.format(
                        job_id, container.id, depend
                    ))
                    logger.debug('Job/{} ({}) - Depend/{}: Adding job to queue'.format(
                        job_id, container.id, depend
                    ))
                    threading.Thread(target=self._queue_job, args=(job_id, cmd, depend), kwargs=kwargs).start()

                else:
                    logger.debug('Job/{} ({}) - Depend/{}: Already created with status: {}'.format(
                        job_id, container.id, depend, container.status
                    ))

                return container.id

            elif container.status == 'exited':

                # Check for exit code
                ret = container.attrs['State']['ExitCode']
                logger.debug('Job/{} - Depend/{}: Dependent returned "{}"'.format(job_id, depend, ret))
                if ret not in (0, None, '0'):
                    # Raise error and bail
                    self._quit_pipeline(job_id, "Dependent job failed: {}".format(container.status), depend)

            else:
                # Raise error and bail
                self._quit_pipeline(job_id, "Dependent job in unexpected state: {}".format(container.status), depend)

        # Run the command
        logger.debug('Job/{}: Submitting job: {}'.format(job_id, cmd))
        container = self._run_container(name=job_id, command=cmd, **kwargs)
        logger.debug('Job/{} ({}): Created container with status'.format(job_id, container.id, container.status))

        return container.id

    def _queue_job(self, job_id, cmd, depend, **kwargs):
        """
        Waits for the prior job to finish and then triggers the passed job.
        :param job_id: The job name for the next job
        :param cmd: The command to run
        :param depend: The job we are waiting for
        """
        logger.debug('Job/{} - Depend/{}: Queuing job behind dependent'.format(job_id, depend))

        # Ensure it exists
        container = self._get_container(depend)
        if not container:
            # Raise error and bail
            self._quit_pipeline(job_id, "Couldn't find dependent job: {}".format(depend), depend)

        # Check if it's created (waiting on a job itself) and poll until it's running or finished
        while container.status == 'created':
            logger.debug('Job/{}: Dependent job "{}" -> "{}"'.format(job_id, depend, container.status))

            # Try again in n seconds
            time.sleep(10)

            # Update
            container = self._get_container(depend)
            if not container:
                # Raise error and bail
                self._quit_pipeline(job_id, "Couldn't find dependent job: {}".format(depend), depend)

        # Container should be running or finished by now, wait for or pull exit code.
        if container.status == 'running' or container.status == 'exited':
            logger.debug('Job/{}: Dependent job "{}" -> "{}"'.format(job_id, depend, container.status))

            # Wait on it and pull the code
            ret = container.wait()['StatusCode']
            if ret not in (0, None, '0'):
                # Raise error and bail
                self._quit_pipeline(job_id, "Dependent job failed: {}".format(container.status), depend)

        else:
            # Raise error and bail
            self._quit_pipeline(job_id, "Dependent job in unexpected state: {}".format(container.status), depend)

        # Run the next one
        return self.job_submit(job_id, cmd, depend, **kwargs)

    def _quit_pipeline(self, job_id, error, depend=None):
        """Use this method to handle a job failure and signal to following jobs to do the same"""
        logger.debug('Job/{} - Depend/{}: Error "{}"'.format(job_id, depend, error))

        # Remove this one if it was created
        self._remove_container(job_id)

        # Fail out
        raise JobSubmissionError(error)

    def all_children(self):
        """Yields all running children remaining"""
        return self._list_containers(status='running')

    def clean_up(self):
        """Create a list of all processes and kills them all"""
        # List running containers
        container_ids = [c.id for c in self._list_containers(status='running')]
        for container_id in container_ids:
            logger.debug('Job/{}: Cleaning up'.format(container_ids))
            self._stop_container(container_id)

        super(DockerJobManager, self).clean_up()
        return container_ids

    def stop(self, job_id):
        """Send a SIGTERM to the job and clean up"""
        logger.debug('Job/{}: Stop'.format(job_id))
        # Check container is running
        container = self._get_container(job_id)
        if not container:
            return None

        # Stop the container
        self._stop_container(container.id)

    @classmethod
    def is_running(cls, pid):
        """Returns true if the process is still running"""
        logger.debug('Job/{}: Is running?'.format(pid))
        container = cls._get_container(pid)
        return container is not None and container.status == 'running'

    @classmethod
    def _docker_datetime(cls, date_string):
        """
        Takes a datetime string as reported by Docker and parse the date,
        convert to local timezone, and return.
        :param date_string: The datetime string
        :return: A timezone-aware datetime object
        """
        try:
            # Docker includes nanoseconds, lop that off
            date = datetime.strptime(date_string.rstrip('Z')[:26], '%Y-%m-%dT%H:%M:%S.%f')

            # Is UTC, set to local timezone and return
            utc_date = date.replace(tzinfo=pytz.UTC)
            local_date = utc_date.astimezone(now().tzinfo)

            return local_date
        except ValueError:
            return None

    def job_status(self, job_id):
        """Returns a dictionary containing status information,
        can only be called once as it will clean up status files!"""
        logger.debug('Job/{}: Check status'.format(job_id))

        # Start with basic status dict
        status = {
            'name': job_id,
        }

        # Get the container
        container = self._get_container(job_id)
        if container:

            # Get dates, or None if not available
            created = self._docker_datetime(container.attrs['Created'])
            if not created:
                logger.error('No created date from container: {}'.format(container.attrs.get('Created')))
                raise JobSubmissionError('Cannot determine created/submitted date of job')

            started = self._docker_datetime(container.attrs['State']['StartedAt'])
            finished = self._docker_datetime(container.attrs['State']['FinishedAt'])
            if finished:
                # If finished, also grab exit code
                exit_code = container.attrs['State']['ExitCode']
                logger.debug('Job/{}: Has finished at "{}" with return of: "{}"'.format(job_id, finished, exit_code))
            else:
                exit_code = None

            # Build status dictionary
            status.update({
                'pid': container.id,
                'status': container.status if exit_code is None else 'finished',
                'submitted': created,
                'started': started,
                'finished': finished,
                'return': exit_code,
                'error': container.logs(stdout=False, timestamps=True),
            })

            # If we got exit code, remove container
            if finished and exit_code is not None:
                self._remove_container(job_id)

            logger.debug('Job/{}: Status: {}'.format(job_id, status['status']))

        return status
