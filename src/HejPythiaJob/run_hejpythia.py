#!/usr/bin/env python
"""
Runs a multiprocessed HEJ+Pythia job on a single grid node.
"""
import argparse
import os
import time
import multiprocessing
from .hejpythia_job import HejPythiaJob


def parse():
    """
    Parse command line arguments.
        user_name : str user handle on gridui and dpm storage
        job_number : int identifying submission
        runs_per_job : int number of runs per submission < 4
        events : int number of events per run
        base_dir : base directory containing run configuration files
        rivet_dir : directory containing rivet analyses
        grid_output_dir : directory on grid storage for output, with protocol
        name : job name
    """
    parser = argparse.ArgumentParser(description = "Usage: python run_hejpythia.py -u user_name -j job_number -p runs_per_job -e events -b base_dir -r rivet_dir -o grid_output_dir")
    parser.add_argument('--user_name', '-u', nargs = 1, type = str)
    parser.add_argument('--job_number', '-j', nargs = 1, type = int, default = 1)
    parser.add_argument('--processes', '-p', nargs = 1, type = int, default = 1)
    parser.add_argument('--events', '-e', nargs = 1, type = int, default = 100)
    parser.add_argument('--base_dir', '-b', nargs = 1, type = str, default = os.getcwd())
    parser.add_argument('--rivet_dir', '-r', nargs = 1, type = str, default = os.getcwd())
    parser.add_argument('--output', '-o', nargs = 1, type = str)
    return parser.parse_args()


def main():
    """
    Run multiple HEJ+Pythia jobs per submission node.
    """
    args = parse()

    t0 = time.time()
    hejpythia = HejPythiaJob(args.user_name[0], args.job_number[0], args.base_dir[0], args.rivet_dir[0], args.output[0])
    hejpythia.set_env()

    if args.processes[0] > 4:
        raise(ValueError("Maximum number of processes is 4 per node."))

    t1 = time.time()
    jobs = []
    for number in range(args.processes[0]):
        p = multiprocessing.Process(target = hejpythia.run_job, args = (number, args.events[0]))
        jobs.append(p)

    for job in jobs:
        job.start()
    
    for job in jobs:
        job.join()

    t2 = time.time()

    print("Environment setting time %s(s)" % (t1 - t0))
    print("Execution time %s(s)" % (t2 - t1))
    print("Total time %s(s)" % (t2 - t0))


if __name__ == """__main__""":
    main()
