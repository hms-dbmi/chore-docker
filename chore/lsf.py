#
# Copyright (C) 2017 Maha Farhat
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
This is the LSF process for submitting jobs to a compute cluster.

This is used by orchestra's compute cloud.
"""

import os

from datetime import datetime
from subprocess import Popen, PIPE

from .base import JobManagerBase, make_aware, settings

def set_lsf_source(source=None):
    """
    This modifies the current working environment, this is VERY BAD
    But it's pretty much a requirement for setting up LSF.
    """
    pipe = Popen(". %s && env -0" % source, stdout=PIPE, shell=True)
    output = pipe.communicate()[0].split('\0')
    env = [line.split("=", 1) for line in output if '=' in line]
    os.environ.update(dict(env))

set_lsf_source(getattr(settings, 'PIPELINE_LSF_SOURCE', None))

class LsfJobManager(JobManagerBase):
    """
    Submits jobs to the IBM lsf (bsub) system
    """
    programs = ['bsub', 'bkill', 'bjobs']

    def __init__(self, *args, **kw):
        self.group = getattr(settings, 'PIPELINE_LSF_GROUP', None)
        self.queue = getattr(settings, 'PIPELINE_LSF_QUEUE', 'short')
        self.limit = getattr(settings, 'PIPELINE_LSF_LIMIT', '12:00')
        if isinstance(self.limit, int):
            self.limit = "%s:00" % self.limit

        super(LsfJobManager, self).__init__(*args, **kw)

    def job_submit(self, job_id, cmd, depend=None, **kw):
        """
        Open the command locally using bash shell.
        """
        bcmd = ['bsub', '-J', job_id, '-q', self.queue]
        if self.group:
            bcmd += ['-g', self.group]
        if depend:
            bcmd += ['-w', 'done(%s)' % depend]
        bcmd += ['-o', self.job_fn(job_id, 'err'), '-W', self.limit, cmd]

        proc = Popen(bcmd, shell=False, stdout=None, stderr=None, close_fds=True)
        return proc.wait() == 0

    @staticmethod
    def stop(job_id):
        """Stop the given process using bkill"""
        return Popen(['bkill', '-J', job_id]).wait() == 0

    def job_status(self, job_id):
        """
        Returns if the job is running, how long it took or is taking
        as well as details status from lsf such as error message.
        """
        # Get the status for the listed job, how long it took and everything
        proc = Popen(['bjobs', '-J', job_id, '-a', '-W'], stdout=PIPE, stderr=None)
        (out, err) = proc.communicate()

        # Prevent spaces in the job_id from hurting the processing
        out = out.replace(job_id, 'job_id_here')

        # Turn the output into a dictionary useful
        lines = out.split('\n')
        if len(lines) <= 1:
            return {}

        data = zip(lines[0].lower().split(), lines[1].split())
        data = dict(data)

        # When getting date-times from lsf, we have to convert and add the year (very odd)
        year = "%d/" % datetime.now().year
        for dkey in data:
            if not dkey.endswith('_time'):
                continue
            if data[dkey] == '-':
                data[dkey] = None
            else:
                data[dkey] = make_aware(
                    datetime.strptime(year + data[dkey], '%Y/%m/%d-%H:%M:%S'))

        status = {
            'RUN': 'running',
            'PEND': 'pending',
            'PSUSP': 'finished',
            'SSUSP': 'finished',
            'DONE': 'finished',
            'EXIT': 'finished',
        }.get(data['stat'])

        (_, err) = self.job_read(job_id, 'err')

        ret, err = self._split_lsf_err(*(err or '').split('-'*60))

        # Just in case
        if ret is None and status == 'finished':
            ret = 404

        return {
            'submitted': data['submit_time'],
            'started': data['start_time'],
            'finished': data['finish_time'],
            'pid': data['jobid'],
            'status': status,
            'return': ret,
            'error': err,
        }

    @staticmethod
    def _split_lsf_err(err, *pages):
        """Split out the lsf output and returns return code and error message"""
        for page in pages:
            # Get return code from output pages
            if 'Resource usage summary' in page:
                if 'Successfully completed' in page:
                    return 0, None
                elif 'Exited with exit code' in page:
                    return int(page.split('exit code ')[-1].split('.')[0]), err

        if 'User defined signal 2' in err:
            err = 'Pipeline stopped by computer cluster for taking too long.'
        return None, err
