#!/usr/bin/env python
"""
A fake sbatch command for chore testing
"""
import os
import sys
sys.path.insert(0, '.')

from datetime import datetime
from random import randint
from argparse import ArgumentParser
from chore.slurm import SlurmJobTester

def main():
    """Run program"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('-d', '--dependency', help='Job chain deps', default=None)
    parser.add_argument('-t', '--time', help='Timeout', default='1:00')
    parser.add_argument('-J', '--job-name', help='Name of the job (optiona)', default=None)
    parser.add_argument('-p', '--partition', help='Parition to use', default='normal')
    parser.add_argument('-e', '--err', help='Where to put the error file', default=None)
    parser.add_argument('-o', '--out', help='Where to put the standard error file', default=None)

    arg = parser.parse_args()
    slurm_db = SlurmJobTester.get_test_database()

    job_id = str(randint(1000000, 9999999))
    os.environ['SLURM_JOB_ID'] = job_id

    row = {
        'id': job_id,
        'name': arg.job_name,
        'partition': arg.parition,
        'error': arg.err,
        'output': arg.out,
        'forward': None,
        'backward': [],
        'submitted': datetime.now(),
        'started': None,
        'finished': None,
    }

    if sys.stdin.isatty():
        row['script'] = sys.stdin.read()

    if arg.dependency:
        _, depid = arg.dependency.split(':')
        if depid not in slurm_db:
            sys.stderr.write("Dependancy error: job_id '{}' not found".format(depid))
            sys.exit(5)

        # Double link up the ids for later use.
        row['forward'] = depid
        slurm_db['depid']['backward'].append(job_id)

    slurm_db[job_id] = row

if __name__ == '__main__':
    main()
