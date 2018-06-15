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
Gets the configured pipeline module and initialises it.
"""

import inspect
from importlib import import_module

from .base import JobManagerBase, PIPELINE_MODULE, PIPELINE_ROOT, PIPELINE_BATCHED, tripplet

__pkgname__ = 'chore'
__version__ = '0.7.6'

def get_job_manager(
        module_id=PIPELINE_MODULE,
        pipe_root=PIPELINE_ROOT,
        batched=PIPELINE_BATCHED):
    """Return the configured job manager for this system"""
    # Already a job manager, so return
    if isinstance(module_id, JobManagerBase):
        return module_id

    # A job manager class, create object and return
    if inspect.isclass(module_id):
        return module_id(pipedir=pipe_root, batch=batched)

    # A name to a job manage, import and create
    try:
        (module, cls) = module_id.rsplit('.', 1)
        module = import_module(module)
        return getattr(module, cls)(pipedir=pipe_root, batch=batched)
    except ImportError:
        raise ImportError("Pipeline module {} is not found.".format(module_id))
    except SyntaxError as err:
        raise ImportError("Pipeline module {} err: {}".format(module_id, err))
    except AttributeError:
        raise ImportError("Pipeline class {} is not found.".format(module_id))
