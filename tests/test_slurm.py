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

from .test_base import ManagerTestBase, watch_log

DIR = os.path.dirname(__file__)
DATA = os.path.join(DIR, 'data')

class SlurmManagerTest(ManagerTestBase):
    """Test running various slurm based jobs"""
    manager_cls = 'chore.slurm.SlurmJobManager'

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

    def assertDependantJobs(self, *cmds, **kw): # pylint: disable=invalid-name
        """Check a chain of jobs and make sure they work in line"""
        expected = kw.pop('expected', None)

        for pos, cmd in enumerate(cmds):
            cmd = 'sleep 0.1 && ' + (cmd % kw)
            depends = 'a%d' % (pos - 1) if pos else None
            self.manager.submit('a%d' % pos, cmd, depends=depends)

        if 'call' in kw:
            kw['call']()

        ret = []
        job = 0
        timeout = len(cmds) * 10
        while job < len(cmds):
            data = self.manager.status('a%d' % job)
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
                raise

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
            self.manager.stop('a0')
        self.assertDependantJobs('sleep 60', 'sleep 1', 'sleep 1', call=stop,\
            expected=('stopped:9', 'stopped:1', 'stopped:1'))
