#!/usr/bin/env python
"""
Runs a multitprocessed temporary job on a single grid node.
"""
import argparse
import os
import time
import multiprocessing


class Job(): 


    def __init__(self, user_name, job_number, base_dir, output_dir):
        """
        Initialises a Job run given:
            user_name : str user name for gridui and dpm grid storage
            job_number : index of the submission
            base_dir : base directory for input files
            output_dir : output directory on grid storage server, with protocol
        """
        self.user_name = str(user_name)
        self.job_number = int(job_number)
        self.base_dir = str(base_dir)
        self.output_dir = str(output_dir)


    def __del__(self):
        """
        Cleanly deletes excess files on destruction.
        """
        self.clean_job()


    def set_env(self):
        """
        Sets the environment for the Job run.
        """
        print("Setting environment for run at:")
        os.system("date")
        cmd = "source /mt/home/%s/bashrc" % (self.user_name)
        os.system(cmd)

        os.environ["MYPROXY_SERVER"] = "myproxy.gridpp.rl.ac.uk"
        print("Environment set at:")
        os.system("date")
        

    def get_unique_seed(self, run_number):
        """
        Generates a unique integer RNG seed with Cantor's pairing function.
        """
        return int(0.5 * (self.job_number + int(run_number)) * (self.job_number + int(run_number) + 1) + int(run_number))


    def run_job(self, run_number, events):
        """
        The main loop for a Job run given the run index on the current node
        and a number of events.
        """
        seed = self.get_unique_seed(run_number)
        cmd = "cp %s/input_files ./input_files%s" % (str(self.base_dir), str(seed))
        os.system(cmd)

        # Run Job
        print("Starting Job run at:")
        os.system("date")
        cmd = "executable --input input_files%s --events %s --output output_files%s" % (str(seed), str(events), str(seed))
        os.system(cmd)
        print("Job finished running at:")
        os.system("date")

        self.print_info()
        self.save_results(seed)


    def save_results(self, seed):
        """
        Copies the output files and input to the grid storage.
        """
        # Compress the output into one tarball
        cmd = "tar -czvf output%s.tar.gz output_file%s* input_files%s*" % (str(seed), str(seed), str(seed))
        os.system(cmd)

        # Copy the tarball of results to the grid storage
        cmd = "gfal-copy output%s.tar.gz %s -f" % (str(seed), str(self.output_dir))
        os.system(cmd)


    def clean_job(self):
        """
        Removes the remaining files.
        """
        os.system("rm *gz input_files* output_files* -f")


    def print_info(self):
        """
        Prints information from the grid node used.
        """
        print("Disk used")
        os.system("du -ksh")
        print("File list")
        os.system("ls -lR")
        print("cpuinfo")
        os.system("cat /proc/cpuinfo")



class JobMerger():


    def __init__(self, user_name, grid_output_dir, prune=False, prune_script="yodastats"):
        """
        Initialises merger for output files given:
            user_name       : user name for gridui and dpm grid storage
            grid_output_dir : location of output files on grid storage
            prune           : optional bool to prune the output data
            prune_script    : name of C/C++ script to prune yoda files
        """
        self.user_name = str(user_name)
        self.grid_output_dir = str(grid_output_dir)
        self.prune = bool(prune)
        if self.prune:
            self.prune_script = prune_script

        self.scratch_dir = "/scratch/%s/tmp_output" % (str(user_name))
        cmd = "mkdir %s" % self.scratch_dir
        os.system(cmd)


    def copy_files(self):
        """
        Copies grid output files to scratch dir.
        """
        print("Copying output to scratch")
        cmd = "gfal-copy -f -r %s %s" % (self.grid_output_dir, self.scratch_dir)
        os.system(cmd)
        os.system("mkdir results")

        print("Organising output into categories of runs")
        files = os.listdir(self.scratch_dir)
        with multiprocessing.Pool() as pool:
            # Use multiprocessing to organise output in parallel
            pool.map(self.organise_single, files)

        os.system("rm tmp_logfile")


    def organise_single(self, filename):
        """
        Organise the tarball of results named 'filename'.
        """
        # TODO: Clean output for log file (e.g. mark with process ID)
        cmd = "tar -xzf %s/%s >> tmp_logfile 2>&1" % (self.scratch_dir, filename)
        os.system(cmd)
        cmd = "mv output_files* results >> tmp_logfile 2>&1"
        os.system(cmd
        cmd = "rm output_files* >> tmp_logfile 2>&1"
        os.system(cmd)


    def merge_output(self):
        """
        Prunes (if set) and merges output files.
        """
        if self.prune:
            self.prune_output()

        # Merge results
        print("Merging output files")
        cmd = "mkdir results/merged"
        os.system(cmd)

        # Add code to merge output

        print("Output files merged")


    def clear_files(self):
        """
        Removes files created in scratch.
        """
        cmd = "rm -r %s" % (self.scratch_dir)
        os.system(cmd)


    def prune_output(self):
        """
        Performs a statistical analysis of the output to remove outliers.
        """
        if not self.prune:
            raise(ValueError("Pruning has not been set."))

        cmd = str(self.prune_script)
        os.system(cmd)


def parse():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description = "Usage: python run_multi.py -u user_name -j job_number -p runs_per_job -e events -b base_dir -o grid_output_dir")
    parser.add_argument('--user_name', '-u', nargs = 1, type = str)
    parser.add_argument('--job_number', '-j', nargs = 1, type = int, default = 1)
    parser.add_argument('--processes', '-p', nargs = 1, type = int, default = 1)
    parser.add_argument('--events', '-e', nargs = 1, type = int, default = 100)
    parser.add_argument('--base_dir', '-b', nargs = 1, type = str, default = os.getcwd())
    parser.add_argument('--output', '-o', nargs = 1, type = str)
    return parser.parse_args()


def main():
    """
    Run multiple HEJ+Pythia jobs per submission node.
    """
    args = parse()

    t0 = time.time()
    grid_job = Job(args.user_name[0], args.job_number[0], args.base_dir[0], args.rivet_dir[0], args.output[0])
    grid_job.set_env()

    if args.processes[0] > 4:
        raise(ValueError("Maximum number of processes is 4 per node."))

    t1 = time.time()
    jobs = []
    for number in range(args.processes[0]):
        p = multiprocessing.Process(target = grid_job.run_job, args = (number, args.events[0]))
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
