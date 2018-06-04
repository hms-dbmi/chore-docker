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
The base functions for a pipeline method manager.
"""

import os
import sys
import atexit
import shutil
import tempfile
from datetime import datetime
from collections import defaultdict

# These sections are for django compatibility
try:
    from django.conf import settings
except ImportError:
    class AttributeDict(dict):
        """Provide access to a dict as object attributes"""
        def __getattr__(self, key):
            try:
                return self[key]
            except KeyError as err:
                raise AttributeError(err)
    settings = AttributeDict(os.environ)

try:
    import subprocess
    from subprocess import NULL # py3k
except ImportError:
    NULL = open(os.devnull, 'wb')

try:
    from django.utils.timezone import make_aware
    from django.utils.timezone import now #pylint: disable=unused-import
except ImportError:
    from pytz import timezone
    make_aware = lambda dt: timezone('UTC').localize(dt, is_dst=None)
    now = datetime.now

PIPELINE_MODULE = getattr(settings, 'PIPELINE_MODULE', 'chore.shell.ShellJobManager')
PIPELINE_ROOT = getattr(settings, 'PIPELINE_ROOT', None)
PIPELINE_BATCHED = getattr(settings, 'PIPELINE_BATCHED', False)

def has_program(program):
    """Returns true if the program is found, false if not"""
    try:
        subprocess.call([program, "--help"], stdout=NULL, stderr=NULL)
    except OSError as err:
        if err.errno == os.errno.ENOENT:
            return False
        raise
    return True

class JobManagerBase(object):
    """Manage any number of pipeline methods such as shell, slurm, lsb, etc"""
    name = property(lambda self: type(self).__module__.split('.')[-1])
    scripts = defaultdict(str)
    programs = []
    links = {}

    def __init__(self, pipedir=None, batch=False):
        """
        Create the job manager, storing temporary job files in pipedir
        and optionally batch responses into a single job script.
        """
        self.batch = batch
        if pipedir is None:
            self.pipedir = tempfile.mkdtemp(prefix='pipeline-')
            atexit.register(self.clean_up)
        else:
            self.pipedir = pipedir

    @classmethod
    def is_enabled(cls):
        """
        Returns True if this manager is enabled, by default
        checks that the list of used programs are installed.
        """
        for program in cls.programs:
            if not has_program(program):
                return False
        return True

    def submit(self, job_id, cmd, depend=None, provide=None):
        """
        Submit a job to the give batching mechanism.
        """
        if not self.batch:
            return self.submit_job(job_id, cmd, depend=depend)
        return self.submit_batch(job_id, cmd, depend=depend, provide=provide)

    def job_fn(self, job_id, ext='pid'):
        """Return the filename of the given job_id and type"""
        if not os.path.isdir(self.pipedir):
            os.makedirs(self.pipedir)
        return os.path.join(self.pipedir, job_id + '.' + ext)

    def clean_up(self):
        """Deletes all data in the piepline directory."""
        if os.path.isdir(self.pipedir):
            shutil.rmtree(self.pipedir)

    def job_read(self, job_id, ext='pid'):
        """Returns the content of the specific job file"""
        filen = self.job_fn(job_id, ext)
        if os.path.isfile(filen):
            with open(filen, 'r') as fhl:
                dtm = datetime.fromtimestamp(os.path.getmtime(filen))
                return (make_aware(dtm), fhl.read().strip())
        else:
            return (None, None)

    def job_clean(self, job_id, ext):
        """Delete files once finished with them"""
        filen = self.job_fn(job_id, ext)
        if os.path.isfile(filen):
            os.unlink(filen)
            return True
        return False

    def job_write(self, job_id, ext, data):
        """Write the data to the given job_id record"""
        filen = self.job_fn(job_id, ext)
        with open(filen, 'w') as fhl:
            fhl.write(str(data))

    def job_stale(self, job_id):
        """Figure out if a job has stale return files"""
        if self.job_clean(job_id, 'ret'):
            sys.stderr.write("Stale job file cleared: {}\n".format(job_id))
            self.job_clean(job_id, 'pid')

    def submit_job(self, job_id, cmd, depend=None):
        """Submit a single job function to this job manager.

        job_id - The identifier for this job, must be historically unique.
        cmd    - The command as you would type it out on a command line.
        depend - The job_id for the previous or 'job this command depends on'
        """
        raise NotImplementedError("Function 'submit_job' is missing.")

    def submit_batch(self, job_id, cmd, depend=None, provide=None):
        """
        Collect together all the commands in this chain into a batch script

        No jobs are dispatched until the last command with no provide id.

        job_id  - The unique identifier for this job, the batch id will be
                  constructed from the common prefix of the first two jobs
                  plus any suffix number or a random number.
        cmd     - The command as you would type it out on a command line.
        depend  - The job_id for the previous or 'job this command depends on'
                  When depend is None, this job is considered the first job in
                  the possible chain of jobs.
        provide - The job_id for the next or 'job this command provides for'
                  When provide is None, this job is considered the last job in
                  the chain of jobs (or the only job if also the first)
                  No jobs as dispatched until a job is submitted without this.
        """
        if depend:
            # This job is not the first in the chain, so get existing script
            script_id = self.links[depend]
        elif not provide:
            # This job is the only job in the chain.
            script_id = job_id
        else:
            # This is the first job and there is more to follow
            script_id = os.path.commonprefix([job_id, provide])
            if not script_id:
                raise KeyError("All jobs must have a unique common prefix.")

        self.links[job_id] = script_id
        self.scripts[script_id] += self._construct_job(job_id, cmd)

        if not provide:
            # This job is the last (or only) job in the list of jobs
            self.submit_job(script_id, self.scripts[script_id])

    def _construct_job(self, job_id, cmd):
        """Turn a job command into one part of a script"""
        return """#   --== JOB: {job_id:s} ==--
echo "-" > {ret:s}
{cmd:s}
echo "$?" > {ret:s}
""".format(job_id=job_id, cmd=cmd, status=self.job_fn(job_id, 'ret'))
