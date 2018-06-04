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

from datetime import datetime
from subprocess import Popen, PIPE

from .base import JobManagerBase, make_aware, settings, NULL

class InvalidPartition(KeyError):
    """When selecting an invalid partition name"""

class SlurmJobManager(JobManagerBase):
    """Manage jobs sent to slurm cluster manager"""
    programs = ['sbatch', 'scancel', 'sacct']

    def __init__(self, *args, **kw):
        self.partition = getattr(settings, 'PIPELINE_SLURM_PARTITION', 'normal')
        self.limit = getattr(settings, 'PIPELINE_SLURM_LIMIT', '12:00')
        if isinstance(self.limit, int):
            self.limit = "%s:00" % self.limit

        super(SlurmJobManager, self).__init__(*args, **kw)

    def job_submit(self, job_id, cmd, depend=None, **kw):
        """
        Open the command locally using bash shell.
        """
        bcmd = ['sbatch', '-J', job_id, '-p', self.partition, '-e', self.job_fn(job_id, 'err')]
        if depend:
            bcmd += ['--dependency=afterok:{}'.format(depend)]
        if self.limit:
            bcmd += ['-t', self.limit]

        # Prefix bash interpriter for script
        cmd = "#!/bin/bash\n\n" + cmd

        proc = Popen(
            bcmd,
            shell=False,
            stdout=NULL,
            stderr=PIPE,
            stdin=PIPE,
            close_fds=True)

        (_, stderr) = proc.communicate(input=cmd)

        if 'invalid partition specified' in stderr:
            raise InvalidPartition(stderr.split("\n")[0].split(': ')[-1])
        elif stderr:
            raise IOError(stderr)

        return proc.wait() == 0

    @staticmethod
    def stop(job_id):
        """Stop the given process using scancel"""
        return Popen(['scancel', job_id]).wait() == 0

    def job_status(self, job_id):
        """Returns if the job is running, how long it took or is taking."""
        # Get the status for the listed job, how long it took and everything
        cmd = ['sacct', '-a', '-p',
               '--format', 'jobid,jobname,submit,start,end,state,exitcode',\
               '--name', job_id]
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()

        # Turn the output into a dictionary useful
        lines = out.strip().split('\n')
        if len(lines) <= 1:
            return {}

        data = dict(zip(lines[0].lower().split('|'), lines[-1].split('|')))

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

        ret = None
        (_, err) = self.job_read(job_id, 'err')
        (ret, sig) = data['exitcode'].split(':')

        return {
            'submitted': data['submit'],
            'started': data['start'],
            'finished': data['end'],
            'pid': data['jobid'],
            'status': status,
            'return': ret,
            'error': int(err or 0),
            'signal': int(sig),
        }
