#!/usr/bin/env python
"""
Runs a multiprocessed Sherpa job on a single grid node.

The code is based on run_hejpythia.py
"""
import argparse
import os
import time
import multiprocessing


class SherpaJob(): 


    def __init__(self, user_name, job_number, base_dir, rivet_dir, output_dir):
        """
        Initialises a Sherpa run given:
            user_name : str user name for gridui and dpm grid storage
            job_number : index of the submission
            base_dir : base directory for input files
            rivet_dir : path for compiled rivet analysis libraries, and PDFs
            output_dir : output directory on grid storage server, with protocol
        """
        self.user_name = str(user_name)
        self.job_number = int(job_number)
        self.base_dir = str(base_dir)
        self.rivet_dir = str(rivet_dir)
        self.output_dir = str(output_dir)


    def __del__(self):
        """
        Cleanly deletes excess files on destruction.
        """
        self.clean_job()


    def set_env(self):
        """
        Sets the environment for a Sherpa run by downloading Sherpa, libSherpaLHEfix.so
        and rivet analyses and setting $PATH and $LD_LIBRARY_PATH and $RIVET_ANALYSIS_PATH.
        """
        print("Setting environment for Sherpa run")
        os.system("date")
        cmd = "source /mt/home/%s/.bashrc" % (self.user_name)
        os.system(cmd)
        os.system("source /cvmfs/pheno.egi.eu/HEJ/HEJ_env.sh")
        os.environ["MYPROXY_SERVER"] = "myproxy.gridpp.rl.ac.uk"

        print("Copying Sherpa.tar.gz from grid storage")
        os.system("mkdir ./Sherpa")
        cmd = "gfal-copy gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/%s/Sherpa/Sherpa.tar.gz . -f" % self.user_name
        os.system(cmd)
        os.environ["RIVET_ANALYSIS_PATH"] = str(self.rivet_dir)
        os.environ["LHAPDF_DATA_PATH"] = str(self.rivet_dir)

        print("untarring Sherpa.tar.gz")
        os.system("tar -xzf Sherpa.tar.gz")
        os.system("mv bin lib include share Sherpa")

        print("Setting Sherpa path variables")
        os.environ["SHERPA_INCLUDE_PATH"] = "%s/Sherpa/include/SHERPA-MC" % str(os.getcwd())
        os.environ["SHERPA_SHARE_PATH"] = "%s/Sherpa/share/SHERPA-MC" % str(os.getcwd())
        os.environ["SHERPA_LIBRARY_PATH"] = "%s/Sherpa/lib/SHERPA-MC" % str(os.getcwd())
        
        print("Copying libSherpaLHEfix.tar.gz from grid storage")
        cmd = "gfal-copy gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/%s/lib/libSherpaLHEfix.tar.gz ./ -f" % self.user_name
        os.system(cmd)
        print("untarring libSherpaLHEfix.tar.gz")
        os.system("tar -xzf libSherpaLHEfix.tar.gz")
        os.environ["SHERPA_LHEFIX_PATH"] = "%s/lib" % str(os.getcwd())
        
        print("Setting environment for Sherpa run")
        os.environ["PATH"] = "%s/Sherpa/bin:%s" % (str(os.getcwd()), str(os.environ.get("PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "%s:%s" % (str(os.environ.get("SHERPA_LIBRARY_PATH",'')), str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "%s:%s" % (str(os.environ.get("SHERPA_LHEFIX_PATH",'')), str(os.environ.get("LD_LIBRARY_PATH",'')))
 
        print("Setting environment for HEJ run - required libraries for Sherpa")
        self.set_hej_env()

        print("Environment set at:")
        os.system("date")


    def set_hej_env(self):
        """
        Sets the HEJ environment
        """
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/boost/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/gcc_9/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/gcc_9/lib64/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/CLHEP/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/fastjet/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/LHAPDF/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/QCDloop/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/rivet/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/yaml-cpp/lib/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        os.environ["LD_LIBRARY_PATH"] = "/cvmfs/pheno.egi.eu/HEJ/HepMC3/lib64/:%s" % (str(os.environ.get("LD_LIBRARY_PATH",'')))
        

    def get_unique_seed(self, run_number):
        """
        Generates a unique integer RNG seed with Cantor's pairing function.
        """
        return int(0.5 * (self.job_number + int(run_number)) * (self.job_number + int(run_number) + 1) + int(run_number))


    def run_job(self, run_number, events):
        """
        The main loop for the Sherpa run given the run index on the current node
        and a number of events.
        """
        # TODO: Don't hardcode names of runfiles (even though they are standard)
        seed = self.get_unique_seed(run_number)
        cmd = "cp -r %s/Results.db %s/Process %s/Run.dat ." % (str(self.base_dir), str(self.base_dir), str(self.base_dir))
        os.system(cmd)

        # Run Sherpa
        print("Starting Sherpa run at:")
        os.system("date")
        cmd = "Sherpa -f Run.dat -R %s -e %s ANALYSIS_OUTPUT=SHERPA-%s EVENT_OUTPUT=LHEfix[SherpaLHE_%s] EVENT_GENERATION_MODE=P USE_GZIP=0" % (str(seed), str(events), str(seed), str(seed))
        os.system(cmd)
        print("Sherpa finished running at:")
        os.system("date")

        self.print_info()
        self.save_results(seed)


    def save_results(self, seed):
        """
        Copies the analysis output files and input cards to the grid storage.
        """
        # Compress the output into one tarball
        cmd = "tar -czvf sherpa_output%s.tar.gz *%s*.yoda *dat" % (str(seed), str(seed))
        os.system(cmd)

        # Copy the tarball of results to the grid storage
        cmd = "gfal-copy sherpa_output%s.tar.gz %s -f" % (str(seed), str(self.output_dir))
        os.system(cmd)


    def clean_job(self):
        """
        Removes the remaining files.
        """
        os.system("rm *gz *yml *dat *yoda *tex *lhe* Results* -r Process Sherpa lib bin include share Status* -f")


    def print_info(self):
        """
        Prints information from the grid node used.
        """
        print("Disk used")
        os.system("du -ksh")
        print("File list")
        os.system("ls -l")
        print("cpuinfo")
        os.system("cat /proc/cpuinfo")



class SherpaMerger():


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

        addendum = os.path.basename(os.path.normpath(grid_output_dir))
        self.scratch_dir = "/scratch/%s/tmp_output_%s" % (str(user_name), str(addendum))
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
        print(files)
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
        cmd = "mv *yoda results >> tmp_logfile 2>&1"
        os.system(cmd)
        cmd = "rm *yoda *cmnd *yml Run.dat >> tmp_logfile 2>&1"
        os.system(cmd)


    def merge_output(self, with_variations = True):
        """
        Prunes (if set) and merges output files.
        """
        if self.prune:
            self.prune_output()

        # Merge Sherpa results
        print("Merging Sherpa yoda files")
        cmd = "mkdir results/merged"
        os.system(cmd)
        if with_variations:
            cmd = "yodamerge results/*.MUR2_MUF2_* -o results/merged/Sherpa-MUR2-MUF2.yoda"
            os.system(cmd)
            # cmd = "yodamerge results/*.MUR1_MUF2_* -o results/merged/Sherpa-MUR1-MUF2.yoda"
            # os.system(cmd)
            # cmd = "yodamerge results/*.MUR2_MUF1_* -o results/merged/Sherpa-MUR2-MUF1.yoda"
            # os.system(cmd)
            # cmd = "yodamerge results/*.MUR0.5_MUF1_* -o results/merged/Sherpa-MUR0.5-MUF1.yoda"
            # os.system(cmd)
            # cmd = "yodamerge results/*.MUR1_MUF0.5_* -o results/merged/Sherpa-MUR1-MUF0.5.yoda"
            # os.system(cmd)
            cmd = "yodamerge results/*.MUR0.5_MUF0.5_* -o results/merged/Sherpa-MUR0.5-MUF0.5.yoda"
            os.system(cmd)
        
        cmd = "rm results/*MU*"
        os.system(cmd)
        cmd = "yodamerge results/*yoda -o results/merged/Sherpa.yoda"
        os.system(cmd)
        cmd = "rm results/*yoda"
        os.system(cmd)
        print("Sherpa yoda files merged")


    def clear_files(self):
        """
        Removes files created in scratch.
        """
        cmd = "rm -r %s" % (self.scratch_dir)
        os.system(cmd)


    def prune_output(self):
        """
        Performs a statistical analysis of the output
        """
        if not self.prune:
            raise(ValueError("Pruning has not been set."))

        cmd = str(self.prune_script)
        os.system(cmd)


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
    parser = argparse.ArgumentParser(description = "Usage: python run_sherpa.py -u user_name -j job_number -p runs_per_job -e events -b base_dir -r rivet_dir -o grid_output_dir")
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
    Run multiple Sherpa jobs per submission node.
    """
    args = parse()

    t0 = time.time()
    sherpa = SherpaJob(args.user_name[0], args.job_number[0], args.base_dir[0], args.rivet_dir[0], args.output[0])
    sherpa.set_env()

    if args.processes[0] > 4:
        raise(ValueError("Maximum number of processes is 4 per node."))

    t1 = time.time()
    jobs = []
    for number in range(args.processes[0]):
        p = multiprocessing.Process(target = sherpa.run_job, args = (number, args.events[0]))
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
