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
This is the most basic way of running jobs, in the local shell.
"""

import os
import signal
import shutil
import docker
from docker.types import Mount
import time
import pytz
from inspect import getmodule
from datetime import datetime
import threading

import logging
logger = logging.getLogger(__name__)

from .base import JobManagerBase, JobSubmissionError, now, make_aware


class DockerJobManager(JobManagerBase):
    """
    The job is submitted to the Docker daemon and is controlled via container id.
    This container id must be stored in the pipeline-shell directory in tmp.
    """
    # Parameters needed for running jobs in Docker
    image_name = 'gentb/pipeline:latest'
    label_key = 'chore'

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

            # Check image to ensure it's a Pipeline job
            if container.attrs['Config']['Image'] in cls.image_name:

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
            logger.debug('Job/container: {} not found'.format(id_or_name))

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
    def _create_container(cls, name, command, depend=None, provide=None, files_path=None, input_files=None, output_files=None):
        """
        Creates a container for running the passed command
        :param input: The location of the file to work on
        :param command: The command to run on the container
        :param depend: The name of the preceding job
        :param provide: The name of the following job
        :param files_path: The path to the files directory
        :param input_files: A list of paths of files this container needs to mount
        :param output_files: A list of paths of files this container needs to place output files in
        :return: The container ID
        :rtype: str
        """
        logger.debug('Job: {} - Run {} - {} - {}'.format(name, files_path, input_files, output_files))
        client = docker.from_env()

        # Find the current container running Gentb and mount its data volume
        mount = None
        for container in client.containers.list():
            if mount:
                break

            # Check volumes
            for mount in container.attrs['Mounts']:
                # Check destination
                if files_path in mount['Destination']:
                    volume = mount['Source']

                    # Prepare mount
                    mount = Mount(target=files_path, source=os.path.basename(os.path.dirname(volume)), type='volume')
                    break

        # TODO: REMOVE THIS !!!!
        # Modify command
        import random
        sleep = random.randint(30, 90)
        if input_files and output_files:
            command = "sh -c 'sleep {} ; {} ; exit 0'".format(
                sleep,
                ' ; '.join(['cp {} {}'.format(input_files[0], o) for o in output_files])
            )
        else:
            command = 'sleep {}'.format(sleep)
        # TODO: REMOVE THIS !!!!

        # Build the container
        container = client.containers.create(
            name=name,
            image=cls.image_name,
            command=command,
            labels={
                'chore': 'true',
            },
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
        :return: The container ID
        :rtype: str
        """
        # Check if created
        container = cls._get_container(name)
        if not container:

            # Create it
            container = cls._create_container(name, command, **kwargs)

        # Start it
        container.start()

        return container.id

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
            logger.debug('Job/{} - Depend/{}: Submitting job after depdendent'.format(job_id, depend))

            # Ensure it exists
            container = self._get_container(depend)
            if not container:
                self._quit_pipeline(job_id, "Couldn't find dependent job: {}".format(container.status), depend)

            # Check if running or created (pending)
            elif container.status == 'running' or container.status == 'created':
                logger.debug('Job/{} - Depend/{}: Dependent status "{}"'.format(job_id, depend, container.status))

                # If we depend on another process and it's not yet finished, create this job, if
                # not already created
                if not self._get_container(job_id):
                    self._create_container(job_id, cmd, **kwargs)

                # Queue it
                logger.debug('Job/{} - Depend/{}: Dependent waiting/running'.format(job_id, depend))
                threading.Thread(target=self._queue_job, args=(job_id, cmd, depend), kwargs=kwargs).start()

                return True

            elif container.status == 'exited':

                # Check for exit code
                ret = container.attrs['State']['ExitCode']
                logger.debug('Job/{} - Depend/{}: Dependent returned "{}"'.format(job_id, depend, ret))
                if ret not in (0, None, '0'):
                    self._quit_pipeline(job_id, "Dependent job failed: {}".format(container.status), depend)
                    return False

            else:
                self._quit_pipeline(job_id, "Dependent job in unexpected state: {}".format(container.status), depend)
                return False

        # Run the command
        logger.debug('Job/{}: Submitting job: {}'.format(job_id, cmd[:40]))
        self._run_container(name=job_id, command=cmd, **kwargs)

        return True

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
            self._quit_pipeline(job_id, "Couldn't find dependent job: {}".format(depend), depend)
            return False

        # Check if it's created (waiting on a job itself) and poll until it's running or finished
        while container.status == 'created':
            logger.debug('Job/{}: Dependent job "{}" -> "{}"'.format(job_id, depend, container.status))

            # Try again in n seconds
            time.sleep(10)

            # Update
            container = self._get_container(depend)
            if not container:
                self._quit_pipeline(job_id, "Couldn't find dependent job: {}".format(depend), depend)
                return False

        # Container should be running or finished by now, wait for or pull exit code.
        if container.status == 'running' or container.status == 'exited':
            logger.debug('Job/{}: Dependent job "{}" -> "{}"'.format(job_id, depend, container.status))

            # Wait on it and pull the code
            ret = container.wait()['StatusCode']
            if ret not in (0, None, '0'):
                self._quit_pipeline(job_id, "Dependent job failed: {}".format(container.status), depend)
                return False

        else:
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
                'status': container.status,
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
