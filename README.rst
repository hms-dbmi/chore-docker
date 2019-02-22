Python Chore
------------

Description
===========

The Chore dispatcher is a multi-job scheduler module for sending jobs to different
systems. It was developed to help manage bioinformatics jobs in a django website
environment. But should equally work well outside of django.

How to Use the Module
=====================

A simple example::

    from chore.shell import JobManager

    manager = JobManager()
    manager.submit('job_1', 'wait 10')

Basic options example::

    manager = JobManager(pipedir='/specific/stash_dir', batch=True)
    manager.submit('job_1', cmd='wait 10')

A slurm example::

    from chore.slurm import JobManager

    manager = JobManager('/tmp/stash_dir', batch=True)
    manager.submit('job_1', 'wait 10', provide='job_2')
    manager.submit('job_2', 'wait 20', depend='job_1')

