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

from .base import JobManagerBase, make_aware, settings, NULL, command

class InvalidPartition(KeyError):
    """When selecting an invalid partition name"""

class SlurmJobManager(JobManagerBase):
    """Manage jobs sent to slurm cluster manager"""
    programs = ['sbatch', 'scancel', 'sacct']

    def __init__(self, *args, **kw):
        self.partition = getattr(settings, 'PIPELINE_SLURM_PARTITION', 'normal')
        self.limit = getattr(settings, 'PIPELINE_SLURM_LIMIT', '12:00')
        self.user = getattr(settings, 'PIPELINE_SLURM_USER', None)
        if isinstance(self.limit, int):
            self.limit = "%s:00" % self.limit

        super(SlurmJobManager, self).__init__(*args, **kw)

    def job_submit(self, job_id, cmd, depend=None, **kw):
        """
        Open the command locally using bash shell.
        """
        bcmd = command('sbatch', J=job_id, p=self.partition,
                       e=self.job_fn(job_id, 'err'),
                       o=self.job_fn(job_id, 'out'))
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

    def jobs_status(self, *args, **kw):
        """Returns the status for the whole slurm directory"""
        for ret in self._sacct(*args, **kw):
            if ret['name'] == 'batch':
                continue
            yield ret

    def job_status(self, job_id):
        """Returns if the job is running, how long it took or is taking."""
        data = list(self._sacct(name=job_id))
        return data[0] if data else {}

    def _sacct(self, *args, **kwargs):
        """Call sacct with the given args and yield dictionary of fields per line"""
        if self.user:
            kwargs['u'] = self.user
        else:
            kwargs['a'] = True
        cmd = command('sacct', p=True, format=[
            'jobid', 'jobname', 'submit', 'start', 'end', 'state', 'exitcode']\
              + list(args), **kwargs)

        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
        (out, _) = proc.communicate()
        lines = out.strip().split('\n')
        header = lines[0].lower().split('|')
        for line in lines[1:]:
            yield self._parse_status(dict(zip(header, line.split('|'))), args)

    def _parse_status(self, data, args):
        # Get the status for the listed job, how long it took and everything
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
