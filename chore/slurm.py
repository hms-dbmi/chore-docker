#
# Copyright (C) 2018 Maha Farhat
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
This is the slurm based work-schedular plugin.
"""

import os
import time

from datetime import datetime
from subprocess import Popen, PIPE

from .base import JobManagerBase, JobSubmissionError, make_aware, settings, command, NULL

class InvalidPartition(KeyError):
    """When selecting an invalid partition name"""

class SlurmJobManager(JobManagerBase):
    """Manage jobs sent to slurm cluster manager"""
    programs = ['sbatch', 'scancel', 'sacct']

    def __init__(self, *args, **kw):
        def get_setting(name, default):
            """Get setting from system, kw or default value"""
            return getattr(settings, 'PIPELINE_SLURM_' + name.upper(),
                           kw.pop(name, default))
        self.partition = get_setting('partition', 'normal')
        self.limit = get_setting('limit', '12:00')
        self.user = get_setting('user', None)
        self.prefix = get_setting('prefix', '#!/bin/bash')

        super(SlurmJobManager, self).__init__(*args, **kw)

    def job_submit(self, job_id, cmd, depend=None, **kw):
        """
        Open the command locally using bash shell.
        """

        bcmd = command('sbatch', J=job_id, p=self.partition,
                       e=self.job_fn(job_id, 'err'),
                       o=self.job_fn(job_id, 'out'))

        if depend is not None:
            child_jobid = self.name_to_id(depend)
            if child_jobid is None:
                raise JobSubmissionError('Could not find dependant job: {}'.format(depend))
            bcmd += ['--dependency=afterok:{}'.format(child_jobid)]

        bcmd += ['--wckey', kw.pop('wckey', '')]
        bcmd += ['--mem', kw.pop('memory', '1000M')]
        bcmd += ['-n', kw.pop('threads', '1')]

        limit = kw.pop('limit', self.limit)
        if limit:
            if isinstance(limit, int):
                limit = "%s:00" % limit
            bcmd += ['-t', limit]

        # Prefix bash interpriter for script
        cmd = self.prefix + "\n\n" + cmd

        proc = Popen(
            bcmd,
            shell=False,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            env=os.environ,
            close_fds=True)

        (stdout, stderr) = proc.communicate(input=cmd)

        if 'invalid partition specified' in stderr:
            raise InvalidPartition(stderr.split("\n")[0].split(': ')[-1])
        elif stderr or proc.wait() != 0:
            raise JobSubmissionError(" ".join(bcmd) + ': ' + stderr)

        slurm_id = stdout.split()[-1]
        self.job_write(job_id, 'sid', slurm_id)
        return slurm_id

    def name_to_id(self, job_name):
        """Slurm uses jobid number, convert one to the other"""
        return self.job_read(job_name, 'sid')

    @staticmethod
    def stop(job_id):
        """Stop the given process using scancel"""
        return Popen(['scancel', job_id], env=os.environ).wait() == 0

    def jobs_status(self, *args, **kw):
        """Returns the status for the whole slurm directory"""
        for ret in self._sacct(*args, **kw):
            if ret['jobname'] == 'batch':
                continue
            yield self._parse_status(ret, args)

    def job_status(self, job_id):
        """Returns if the job is running, how long it took or is taking."""
        for row in self._sacct(name=job_id, a=True):
            return self._parse_status(row)
        return {}

    def _sacct(self, *args, **kwargs):
        """Call sacct with the given args and yield dictionary of fields per line"""
        if 'user' not in kwargs and 'u' not in kwargs and 'a' not in kwargs:
            if self.user:
                kwargs['u'] = self.user
            else:
                kwargs['a'] = True

        if 'format' not in kwargs:
            kwargs['format'] = ['jobid', 'jobname', 'submit', 'start',
                                'end', 'state', 'exitcode'] + list(args)

        cmd = command('sacct', p=True, **kwargs)

        proc = Popen(cmd, stdout=PIPE, stderr=NULL, env=os.environ)
        (out, _) = proc.communicate()
        lines = out.strip().split('\n')
        header = lines[0].lower().split('|')
        for line in lines[1:]:
            yield dict(zip(header, line.split('|')))

    def _parse_status(self, data, args=()):
        """Get the status for the listed job, how long it took and everything"""
        for dkey in ('submit', 'start', 'end'):
            if ':' in data[dkey]:
                data[dkey] = make_aware(
                    datetime.strptime(data[dkey], '%Y-%m-%dT%H:%M:%S'))
            else: # Should be 'Unknown', no reason not to catch all
                data[dkey] = None

        status = {
            'PENDING': 'pending',
            'RUNNING': 'running',
            'SUSPENDED': 'running',
        }.get(data['state'], 'finished')

        (_, err) = self.job_read(data['jobname'], 'err')
        (ret, sig) = data['exitcode'].split(':')

        extras = dict(zip(args, [data.get(arg.lower(), '') for arg in args]))

        if 'CANCELLED'in data['state']:
            extras['return'] = 129
            extras['message'] = 'CANCELLED'
        elif 'TIMEOUT' in data['state']:
            extras['return'] = 130
            extras['message'] = 'TIMEOUT'
        else:
            extras['return'] = int(ret or -1)

        return dict(
            name=data['jobname'],
            submitted=data['submit'],
            started=data['start'],
            finished=data['end'],
            pid=data['jobid'],
            status=status,
            error=str(err),
            signal=int(sig),
            **extras)
