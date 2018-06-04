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
import tempfile

import unittest

from chore import get_job_manager
from chore.watch import LOG as watch_log

DIR = os.path.dirname(__file__)
DATA = os.path.join(DIR, 'data')

class JobManagerTest(unittest.TestCase):
    """Test running various shell based jobs"""
    def setUp(self):
        super(JobManagerTest, self).setUp()
        self.pipes = os.path.join(self.media_root, 'pipe')
        self.manager = get_job_manager('chore.fake', pipe_root=self.pipes)
        self.filename = tempfile.mktemp(prefix='test-job-')

    def tearDown(self):
        super(JobManagerTest, self).tearDown()
        for count in range(20):
            filename = "%s.%d" % (self.filename, count)
            if os.path.isfile(filename):
                os.unlink(filename)
        if os.path.isfile(self.filename):
            os.unlink(self.filename)
        if os.path.isfile(watch_log):
            os.unlink(watch_log)

        self.manager.clean_up()

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
