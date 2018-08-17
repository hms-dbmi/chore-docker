
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

try:
    from itertools import tee, islice, chain, izip
except ImportError:
    from itertools import tee, islice, chain # py3
    izip = zip

class JobSubmissionError(IOError):
    """Submit job failed with a message"""

# These sections are for django compatibility
try:
    from django.conf import settings
    from django.core.exceptions import ImproperlyConfigured
    try:
        getattr(settings, 'SITE_ROOT', None)
    except ImproperlyConfigured as err:
        raise ImportError(err)
    from django.utils.timezone import make_aware
    from django.utils.timezone import now #pylint: disable=unused-import
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
        from pytz import timezone
        make_aware = lambda dt: timezone('UTC').localize(dt, is_dst=None)
    except ImportError:
        make_aware = lambda dt: dt #pylint: disable=invalid-name
    now = datetime.now

try:
    import subprocess
    from subprocess import NULL # py3k
except ImportError:
    NULL = open(os.devnull, 'wb')

def has_program(program):
    """Returns true if the program is found, false if not"""
    try:
        subprocess.call([program, "--help"], stdout=NULL, stderr=NULL)
    except OSError as err:
        if err.errno == os.errno.ENOENT:
            return False
        raise
    return True

def tripplet(iterable):
    """Split a list of items into it's previous, current and next items"""
    prevs, items, nexts = tee(iterable, 3)
    prevs = chain([None], prevs)
    nexts = chain(islice(nexts, 1, None), [None])
    return izip(prevs, items, nexts)

class JobManagerBase(object):
    """Manage any number of pipeline methods such as shell, slurm, lsb, etc"""
    name = property(lambda self: type(self).__module__.split('.')[-1])
    scripts = defaultdict(str)
    cached_scripts = {}
    fn_list = set()
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

    def submit(self, job_id, cmd, **kw):
        """
        Submit a job to the give batching mechanism.
        """
        if not self.batch:
            return self.job_submit(job_id, cmd, **kw)
        return self.submit_batch(job_id, cmd, **kw)

    def job_fn(self, job_id, ext='pid'):
        """Return the filename of the given job_id and type"""
        if not os.path.isdir(self.pipedir):
            os.makedirs(self.pipedir)
        self.fn_list.add(ext)
        return os.path.join(self.pipedir, job_id + '.' + ext)

    def jobs(self, ext='pid', **kw):
        """Generator yielding job_ids based on files in job_fn"""
        if os.path.isdir(self.pipedir):
            for item in os.listdir(self.pipedir):
                if item.endswith('.' + ext):
                    yield item.replace('.' + ext, '')

    def job_clean(self, job_id):
        """Remove any remaining files for this old job"""
        for ext in list(self.fn_list):
            self.job_clean_fn(job_id, ext)

    def job_clean_fn(self, job_id, ext):
        """Delete files once finished with them"""
        filen = self.job_fn(job_id, ext)
        if os.path.isfile(filen):
            os.unlink(filen)
            return True
        return False

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

    def job_write(self, job_id, ext, data):
        """Write the data to the given job_id record"""
        filen = self.job_fn(job_id, ext)
        with open(filen, 'w') as fhl:
            fhl.write(str(data))

    def job_stale(self, job_id):
        """Figure out if a job has stale return files"""
        if self.job_clean_fn(job_id, 'ret'):
            sys.stderr.write("Stale job file cleared: {}\n".format(job_id))
            self.job_clean(job_id)

    def job_submit(self, job_id, cmd, depend=None, **kw):
        """Submit a single job function to this job manager.

        job_id - The identifier for this job, must be historically unique.
        cmd    - The command as you would type it out on a command line.
        depend - The job_id for the previous or 'job this command depends on'
        """
        raise NotImplementedError("Function 'job_submit' is missing.")

    def submit_chain(self, chain_id, *jobs):
        """
        Submits many jobs under this chain id.
        """
        ids = ["{}.{}".format(chain_id, x) for x in range(len(jobs))]
        for (pid, job_id, nid), cmd in zip(tripplet(ids), jobs):
            if self.submit(job_id, cmd, depend=pid, provide=nid, chain_id=chain_id) is False:
                return False
        return True

    def submit_batch(self, job_id, cmd, depend=None, provide=None, chain_id=None): # pylint: disable=too-many-arguments
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
        chain_id - Unique id just for this chain, used internally 4 chain jobs.

        Returns:

            str  - A generated chain_id from the job_id, nothing has been
                    dispatched yet.
            True - The batched jobs have been dispatched sucessfully.
            False - The batched jobs failed to be dispatched.

        """
        if chain_id is None:
            if depend:
                # This job is not the first in the chain, so get existing script
                chain_id = self.links[depend]
            elif not provide:
                # This job is the only job in the chain.
                chain_id = job_id
            else:
                # This is the first job and there is more to follow
                chain_id = os.path.commonprefix([job_id, provide])
                if not chain_id:
                    raise KeyError("All jobs must have a unique common prefix.")

        self.links[job_id] = chain_id
        self.scripts[chain_id] += self._construct_job(job_id, cmd)

        if not provide:
            # This job is the last (or only) job in the list of jobs
            script = self.scripts.pop(chain_id)
            self.cached_scripts[chain_id] = script
            return self.job_submit(chain_id, script)
        return chain_id

    def _construct_job(self, job_id, cmd):
        """Turn a job command into one part of a script"""
        return """#   --== JOB: {job_id:s} ==--
echo "-" > {pid:s}
{cmd:s}
echo "$?" > {ret:s}

""".format(
    job_id=job_id,
    cmd=cmd,
    pid=self.job_fn(job_id, 'pid'),
    ret=self.job_fn(job_id, 'ret'))

    def job_status(self, job_id):
        """Returns the status of the job"""
        raise NotImplementedError("Function 'job_status' is missing.")

    def jobs_status(self, *args, **kw):
        """Returns a list of statuses for all recorded jobs"""
        for job_id in self.jobs(*args, **kw):
            yield self.job_status(job_id)

    def status(self, job_id, clean=False):
        """
        Return the status of this job or this batch job
        """
        if self.batch and job_id in self.links:
            chain_id = self.links[job_id]
            chain_ret = self.job_status(chain_id)
            ret = self._program_status(job_id)
            ret['error'] = chain_ret['error']
            return ret
        else: # job_id is chain_id or not batch
            ret = self.job_status(job_id)

        if clean and (ret.get('finished', False) or ret.get('error', False)):
            self.job_clean(job_id)

        return ret

    def _program_status(self, job_id):
        (started, pid) = self.job_read(job_id, 'pid')
        (finished, ret) = self.job_read(job_id, 'ret')
        (_, error) = self.job_read(job_id, 'err')
        status = 'finished'
        if error == 'S':
            error = ''
            if ret != '0':
                status = 'stopped'

        if pid is None and ret is None:
            return {}

        return {
            'name': job_id,
            'pid': pid,
            'status': status,
            'submitted': started,
            'started': started,
            'finished': finished,
            'return': int(ret) if ret is not None else None,
            'error': error,
        }

def compile_value(value):
    """Compile a value for args"""
    if isinstance(value, (list, tuple)):
        return ",".join([str(v) for v in value])
    return str(value)

def compile_args(*args, **kwargs):
    """Compile arguments into a shell standard format, returns a list"""
    for (name, value) in kwargs.items():
        if len(name) == 1:
            yield "-{}".format(name)
        else:
            yield "--{}".format(name)
        if value is not True:
            yield compile_value(value)
    for value in args:
        yield compile_value(value)

def command(command_name, *args, **kwargs):
    """Generate a Popen command list"""
    return [command_name] + list(compile_args(*args, **kwargs))
