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
"""
Test some basic assumptions in the base module
"""

import os
import sys
import time
import shutil
import unittest
import tempfile

from chore import get_job_manager
from chore.watch import LOG as watch_log # pylint: disable=unused-import

DIR = os.path.dirname(__file__)
DATA = os.path.join(DIR, 'data')

class ManagerTestBase(unittest.TestCase):
    """Test running various shell based jobs"""
    manager_cls = None
    batched = False

    def setUp(self):
        super(ManagerTestBase, self).setUp()
        self.tempdir = tempfile.mkdtemp(suffix='chore-tests')
        self.manager = get_job_manager(self.manager_cls, self.tempdir, self.batched)
        if not self.manager.is_enabled():
            self.skipTest("Manager {} is not enabled".format(self.manager_cls))
        self.filename = tempfile.mktemp(prefix='test-job-')

    def tearDown(self):
        super(ManagerTestBase, self).tearDown()
        if os.path.exists(self.tempdir):
            shutil.rmtree(self.tempdir)

    def assertDependantJobs(self, *cmds, **kw): # pylint: disable=invalid-name
        """Check a chain of jobs and make sure they work in line"""
        expected = kw.pop('expected', None)

        cmds = ['sleep 0.1 && ' + (cmd % kw) for cmd in cmds]
        self.manager.submit_chain('a', *cmds)

        if 'call' in kw:
            kw['call']()

        ret = []
        job = 0
        timeout = len(cmds) * 10
        while job < len(cmds):
            data = self.manager.status('a.{}'.format(job))
            if data.get('status', None) in ('finished', 'stopped', None):
                ret.append("%s:%s" % (data.get('status', 'No'), str(data.get('return', -1))))
                job += 1
                continue
            time.sleep(0.1)
            timeout -= 1
            self.assertTrue(timeout > 0, "Timeout waiting for dependant job %s" % cmds[job])

        if expected is not None:
            try:
                self.assertEqual(tuple(expected), tuple(ret))
            except AssertionError:
                if os.path.isfile(watch_log):
                    with open(watch_log, 'r') as fhl:
                        sys.stderr.write("WATCH LOG:\n{}\n\n".format(fhl.read()))
                    os.unlink(watch_log)
                raise

class TestFakeManager(ManagerTestBase):
    """Test some non-comittal code"""
    manager_cls = 'chore.fake.FakeJobManager'
