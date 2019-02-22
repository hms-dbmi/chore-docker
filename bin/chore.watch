#!/usr/bin/env python
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
Companion to shell.py, script to watch for a returned value
"""

import os
import sys
import time
import atexit

LOG = '/tmp/watch.log'

def wait(ret, cadence=0.01):
    """Wait for the return file to be created."""
    name = os.path.basename(ret)
    with open(LOG, 'a') as fhl:
        fhl.write(" + {} ({:0.2f})\n".format(name, float(cadence)))
        fhl.flush()

    count = 0

    def died():
        """Catch any killing instructions"""
        if count >= 0:
            with open(LOG, 'a') as fhl:
                fhl.write(" ! {} (KILLED {:d} waits)\n".format(name, count))

    atexit.register(died)

    while not os.path.isfile(ret):
        count += 1
        time.sleep(float(cadence))

    with open(ret, 'r') as fhl:
        with open(LOG, 'a') as log:
            act = int(fhl.read().strip())
            log.write(" - {} ({:d}, {:d} waits)\n".format(name, act, count))
            count = -1
            # Return 0 if act is 0, or 1 if act is NOT 0
            sys.exit(int(bool(act)))


if __name__ == '__main__':
    wait(ret=sys.argv[1], *sys.argv[2:])
