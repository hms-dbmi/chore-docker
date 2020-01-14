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
import boto3
import botocore
from datetime import datetime

import logging
logger = logging.getLogger(__name__)

from .base import JobManagerBase, JobSubmissionError, now, make_aware


class BatchJobManager(JobManagerBase):
    """
    The job is submitted to AWS Batch as jobs in a queue.
    """
    @classmethod
    def get_batch_queue(cls):
        """
        Fetches the name of the job queue to use for Batch, looks in environment
        :return: str
        """
        return os.environ.get('GENTB_BATCH_JOB_QUEUE', 'gentb-demo-batch-batch-job-queue')

    @classmethod
    def get_batch_job(cls):
        """
        Fetches the name of the job definition to use for Batch, looks in environment
        :return: str
        """
        return os.environ.get('GENTB_BATCH_JOB_DEFINITION', 'gentb-demo-batch-gentb-job')

    @classmethod
    def _list_jobs(cls, status=None):
        """
        Fetches all current job jobs. If passed, returned jobs
        are filtered on their current status (e.g. 'SUBMITTED', 'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')
        :param queue: The name of the job queue to list for
        :rtype: list
        """
        client = boto3.client('batch')
        try:
            # Loop until we pull all jobs
            jobs = []

            # If not a specific status, do them all
            if status:
                job_statuses = [status.upper()]
            else:
                job_statuses = ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED']
            for job_status in job_statuses:

                # Iterate pages, if any
                next_token = None
                while True:

                    # Build arguments
                    kwargs = {}
                    if next_token:
                        kwargs['nextToken'] = next_token

                    # Make the call
                    response = client.list_jobs(
                        jobQueue=cls.get_batch_queue(),
                        jobStatus=job_status,
                        maxResults=100,
                        **kwargs
                    )

                    # Add jobs
                    jobs.extend(response['jobSummaryList'])

                    # Check for more
                    next_token = response.get('nextToken')
                    if not next_token:
                        break

            logger.debug('Found {} jobs for queue: {}{}'.format(
                len(jobs),
                cls.get_batch_queue(),
                ', status: {}'.format(status.upper()) if status else ''
            ))

            return jobs

        except botocore.exceptions.ClientError as e:
            logger.exception('Batch error: {}'.format(e), exc_info=True)
            raise JobSubmissionError('Batch client error')

    @classmethod
    def _get_job(cls, job_id):
        """
        Fetches the job for the id and returns its description dictionary
        :rtype: dict
        """
        try:
            # Get the current list
            jobs = cls._list_jobs()

            # Check if in there
            for job in jobs:

                # Compare name
                if job.get('jobName') == job_id:
                    return job

            # If we got here, job does not exist
            logger.debug('Job/{}: Not found in queue: {}'.format(job_id, cls.get_batch_queue()))

        except botocore.exceptions.ClientError as e:
            logger.exception('Batch error: {}'.format(e), exc_info=True)
            raise JobSubmissionError('Batch client error')

        return None

    @classmethod
    def _stop_job(cls, job_id, reason='Job is terminated because job is terminated'):
        """
        Terminated the job for whatever reason
        :param job_id: The id of the job
        :param reason: The reason for termination of the job
        :returns Whether the job was removed or not
        """
        client = boto3.client('batch')
        try:
            # Get job
            job = cls._get_job(job_id=job_id)

            client.terminate_job(
                jobId=job['jobId'],
                reason=reason
            )

            return True
        except botocore.exceptions.ClientError as e:
            logger.exception('Batch error: {}'.format(e), exc_info=True)

        return False

    @classmethod
    def _run_job(cls, name, command, depend=None, provide=None, files_path=None, input_files=None, output_files=None):
        """
        Creates a job for running the passed command
        :param command: The command to run on the job
        :param depend: The name of the preceding job
        :param provide: The name of the following job
        :param files_path: The path to the files directory
        :param input_files: A list of paths of files this job needs to mount
        :param output_files: A list of paths of files this job needs to place output files in
        :return: The job ID
        :rtype: str
        """
        logger.debug('Job: {} - Run {} - {} - {}'.format(name, files_path, input_files, output_files))
        client = boto3.client('batch')
        try:
            # TODO: REMOVE THIS !!!!
            # Modify command
            import random
            sleep = random.randint(30, 90)
            if input_files and output_files:
                command = [
                    "sh",
                    "-c",
                    "'sleep {} ; ls -la /mnt ; ls -la /mnt/data ; {} ; exit 0'".format(
                        sleep,
                        ' ; '.join(['cp {} {}'.format(input_files[0], o) for o in output_files])
                    )
                ]
            else:
                command = ['sleep', sleep]
            # TODO: REMOVE THIS !!!!

            # Build optional kwargs
            kwargs = {}

            # If dependency is specified, add that
            if depend:
                kwargs['dependsOn'] = [
                    {
                        'jobId': depend,
                        'type': 'SEQUENTIAL'
                    }
                ]

            # Make the request
            response = client.submit_job(
                jobName=name,
                jobQueue=cls.get_batch_queue(),
                jobDefinition=cls.get_batch_job(),
                retryStrategy={
                    'attempts': 3
                },
                containerOverrides={
                    'command': command,
                },
                **kwargs
            )

            return response['jobId']

        except botocore.exceptions.ClientError as e:
            logger.exception('Batch error: {}'.format(e), exc_info=True)

        return None

    def job_submit(self, job_id, cmd, depend=None, **kwargs):
        """
        Submits the job to Batch, specifying dependencies, if any.
        """
        logger.debug('Job/{}: Submitting job: {}'.format(job_id, cmd[:40]))

        return self._run_job(name=job_id, command=cmd, depend=depend, **kwargs)

    def _quit_pipeline(self, job_id, error, depend=None):
        """Use this method to handle a job failure and signal to following jobs to do the same"""
        logger.debug('Job/{} - Depend/{}: Error "{}"'.format(job_id, depend, error))

        # Remove this one if it was created
        self._stop_job(job_id)

        # Fail out
        raise JobSubmissionError(error)

    def all_children(self):
        """Yields all running children remaining"""
        return self._list_jobs(status='RUNNING')

    def clean_up(self):
        """Create a list of all processes and kills them all"""
        # List running jobs
        job_ids = [c.id for c in self._list_jobs(status='RUNNING')]
        for job_id in job_ids:
            logger.debug('Job/{}: Cleaning up'.format(job_ids))
            self._stop_job(job_id)

        super(BatchJobManager, self).clean_up()
        return job_ids

    def stop(self, job_id):
        """Send a SIGTERM to the job and clean up"""
        logger.debug('Job/{}: Stop'.format(job_id))
        # Check job is running
        job = self._get_job(job_id)
        if not job:
            return None

        # Stop the job
        self._stop_job(job_id)

    @classmethod
    def is_running(cls, job_id):
        """Returns true if the process is still running"""
        logger.debug('Job/{}: Is running?'.format(job_id))
        job = cls._get_job(job_id)
        return job is not None and job['status'] in ['RUNNING', 'STARTING']

    @classmethod
    def _batch_datetime(cls, timestamp):
        """
        Takes a datetime string as reported by Batch and parse the date,
        convert to local timezone, and return.
        :param timestamp: The datetime string
        :return: A timezone-aware datetime object
        """
        try:
            # Check for empty timestamps
            if not timestamp:
                return None

            # Batch includes microseconds, lop that off
            date = datetime.fromtimestamp(timestamp / 1000)

            # Is UTC, set to local timezone and return
            local_date = date.replace(tzinfo=now().tzinfo)

            return local_date

        except ValueError:
            return None

    def job_status(self, job_id):
        logger.debug('Job/{}: Check status'.format(job_id))

        # Start with basic status dict
        status = {
            'name': job_id,
        }

        # Get the job
        job = self._get_job(job_id)
        if job:

            # Build status dictionary
            status.update({
                'pid': job['jobName'],
                'jobId': job['jobId'],
                'status': job['status'],
                'submitted': self._batch_datetime(job['createdAt']),
                'started': self._batch_datetime(job.get('startedAt')),
                'finished': self._batch_datetime(job.get('stoppedAt')),
                'return': 0 if job['status'] == 'SUCCEEDED' else 1 if job['status'] == 'FAILED' else None,
                'error': job.get('statusReason'),
            })

            logger.debug('Job/{}: Status: {}'.format(job_id, status['status']))

        return status
