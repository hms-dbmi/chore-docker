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
import time

from .test_base import ManagerTestBase

DIR = os.path.dirname(__file__)
DATA = os.path.join(DIR, 'data')

class ShellManagerTest(ManagerTestBase):
    """Test running various shell based jobs"""
    manager_cls = 'chore.shell.ShellJobManager'

    def test_shell_run(self):
        """Test that jobs can be run via the shell"""
        self.manager.submit('sleep_test_1', 'sleep 60')
        data = self.manager.status('sleep_test_1')
        self.assertIn(data['status'], ('sleeping', 'running'))
        self.manager.stop('sleep_test_1')
        data = self.manager.status('sleep_test_1')
        self.assertEqual(data['status'], 'stopped')

    def test_non_existant_id(self):
        """what happens when the job doesn't exist"""
        data = self.manager.status('sleep_test_0')
        self.assertEqual(data, {})
        self.manager.stop('sleep_test_0')

    def test_error_output(self):
        """When a command returns a non-zero status"""
        self.manager.submit('no_cmd', 'fidly dee')
        data = self.manager.status('no_cmd')
        while data['status'] == 'running':
            data = self.manager.status('no_cmd')
        self.assertEqual(data['status'], 'finished')
        self.assertEqual(data['error'], '/bin/sh: 1: fidly: not found')
        self.assertEqual(data['return'], 127)

    def test_dependant_jobs(self):
        """When one job needs a first job to complete"""
        self.assertDependantJobs(
            'ls --help > %(fn)s.1',
            'grep OK %(fn)s.1 > %(fn)s.2',
            'wc %(fn)s.2 > %(fn)s.3',
            fn=self.filename,
            expected=('finished:0',)*3)

        with open(self.filename+'.1', 'r') as fhl:
            self.assertTrue('Usage' in fhl.read())
        with open(self.filename+'.2', 'r') as fhl:
            self.assertEqual(fhl.read(), ' 0  if OK,\n')
        with open(self.filename+'.3', 'r') as fhl:
            self.assertEqual(fhl.read(), ' 1  3 11 %s.2\n' % self.filename)

    def test_dependant_error(self):
        """When the first job causes an error"""
        self.assertDependantJobs(
            'ls %(fn)s.0 > %(fn)s.1',
            'ls %(fn)s.1 > %(fn)s.2',
            'ls %(fn)s.2 > %(fn)s.3',
            fn=self.filename,
            expected=('finished:2', 'stopped:1', 'stopped:1'))

    def test_dependant_stopped(self):
        """When the first job is stoppped whole chain is stopped"""
        def stop():
            """Quit the job after 100ms"""
            time.sleep(0.1)
            self.manager.stop('a.0')
        self.assertDependantJobs('sleep 60', 'sleep 1', 'sleep 1', call=stop,\
            expected=('stopped:9', 'stopped:1', 'stopped:1'))

class BatchedShellManagerTest(ManagerTestBase):
    """Test running batched up shell deps"""
    manager_cls = 'chore.shell.ShellJobManager'
    batched = True

    def test_simple_chain(self):
        """A single chain of commands to be inserted"""
        ret = self.manager.submit_chain('s', 'sleep 10', 'sleep 5', 'sleep 8')
        self.assertTrue(ret)

        self.assertEqual(
            self.manager.cached_scripts['s'].replace(self.tempdir, '$dir'),
            """#   --== JOB: s.0 ==--
echo "-" > $dir/s.0.pid
sleep 10
echo "$?" > $dir/s.0.ret

#   --== JOB: s.1 ==--
echo "-" > $dir/s.1.pid
sleep 5
echo "$?" > $dir/s.1.ret

#   --== JOB: s.2 ==--
echo "-" > $dir/s.2.pid
sleep 8
echo "$?" > $dir/s.2.ret

""")
