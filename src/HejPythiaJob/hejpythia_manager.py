#!/usr/bin/env python
import os
from run_hejpythia import HejPythiaJob, HejPythiaMerger
import argparse


def make_job_file(user_name, job_number, events, processes, base_dir, rivet_dir, output_dir, name):
    """
    Creates xrsl submission file given:
        job_number : int between n_min and n_max (inclusive)
        events : int number of events per run
        processes : int number of runs per submission < 4
        base_dir : base directory containing run configuration files
        rivet_dir : directory containing rivet analyses
        output_dir : directory on grid storage for output, with protocol
        name : job name
    """
    print("Writing job%s.jdl" % (job_number))
    cmd = """echo "&(executable = '%s')\n""" % (name)
    cmd += """(arguments = '-u' '%s' '-j' '%s' '-p' '%s' '-e' '%s' '-b' '%s' '-r' '%s' '-o' '%s')\n""" % (user_name, job_number, processes, events, base_dir, rivet_dir, output_dir)
    cmd += """(jobname = %s.%s)\n""" % (name, job_number)
    cmd += """(stdout = 'stdout')\n(stderr = 'stderr')\n(gmlog = 'job%s.log')\n""" % (job_number)
    cmd += """(count = '%s')\n(countpernode = '%s')" """ % (processes, processes)
    cmd += """> job%s.jdl""" % (job_number)
    os.system(cmd)


def run(args, write_only = False):
    """
    Submits n_max - n_min + 1 multiprocessed xrsl job scripts to the grid
    unless write_only is set --- then only xrsl input files are written.
    """
    for idx in range(args["n_min"], args["n_max"] + 1):
        make_job_file(args["user_name"], idx, args["events"], args["processes"],
                      args["base_dir"], args["rivet_dir"],
                      args["output_dir"], args["job_name"])

        if not write_only:
            if (idx%2) == 0:
                cmd = "arcsub --direct -c ce1.dur.scotgrid.ac.uk -j ./multijobs.dat job%s.jdl &" % (idx)
                os.system(cmd)

            # Sleep after every other job submission.
            else:
                cmd = "arcsub --direct -c ce1.dur.scotgrid.ac.uk -j ./multijobs.dat job%s.jdl &" % (idx)
                os.system(cmd)
                cmd = "sleep 0.2"
                os.system(cmd)

    cmd = "sleep 0.5"
    os.system(cmd)

    if not write_only:
        cmd = "rm *jdl"
        os.system(cmd)


def main(args):
    """
    Main method for manager functionality.
    """
    parser = argparse.ArgumentParser(description = "Usage: python hejpythia_manager.py [-w] [--write] -r [--run] -s [-status] -f [--finalise] -m [--merge] -c [--clean] -k [--kill]")
    parser.add_argument('--write', '-w', action = "store_true")
    parser.add_argument('--run', '-r', action = "store_true")
    parser.add_argument('--status', '-s', action = "store_true")
    parser.add_argument('--finalise', '-f', action = "store_true")
    parser.add_argument('--merge', '-m', action = "store_true")
    parser.add_argument('--clean', '-c', action = "store_true")
    parser.add_argument('--kill', '-k', action = "store_true")
    manager_args = parser.parse_args()

    if manager_args.run or manager_args.write:
         run(args, manager_args.write)
         return

    if manager_args.status:
         print("Writing job statuses to logfile.txt")
         os.system("rm logfile.txt")
         job_statuses = os.popen("arcstat -j multijobs.dat").read()

         n_running = os.popen("arcstat -j multijobs.dat | grep -i 'Running' | wc -l").read()
         n_finished = os.popen("arcstat -j multijobs.dat | grep -i 'Finished' | wc -l").read()
         n_finishing = os.popen("arcstat -j multijobs.dat | grep -i 'Finishing' | wc -l").read()
         n_failed = os.popen("arcstat -j multijobs.dat | grep -i 'Failed' | wc -l").read()
         n_queueing = os.popen("arcstat -j multijobs.dat | grep -i 'Queueing' | wc -l").read()
         n_missing = os.popen("arcstat -j multijobs.dat | grep -i 'Waiting' | wc -l").read()
         n_tot = int(n_running) + int(n_finished) + int(n_finishing) + int(n_failed) + int(n_queueing) + int(n_missing)

         with open("logfile.txt", "a") as logfile:
             logfile.write("=" * 80 + "\n")
             logfile.write("-" * 32 + " JOB INFORMATION " + "-" * 31 + "\n")
             logfile.write("=" * 80 + "\n")
             logfile.write(job_statuses)
             logfile.write("\n" + "=" * 80 + "\n")
             logfile.write("-" * 30 + " SUMMARY INFORMATION " + "-" * 29 + "\n")
             logfile.write("=" * 80 + "\n")
             logfile.write("Total jobs: %s\n" % str(n_tot))
             logfile.write("Number of running jobs: %s\n" % str(n_running))
             logfile.write("Number of finished jobs: %s\n" % str(n_finished))
             logfile.write("Number of finishing jobs: %s\n" % str(n_finishing))
             logfile.write("Number of failed jobs: %s\n" % str(n_failed))
             logfile.write("Number of queueing jobs: %s\n" % str(n_queueing))
             logfile.write("Number of missing jobs: %s\n" % str(n_missing)) 

         return

    if manager_args.clean:
        os.system("arcclean -j multijobs.dat")
        return

    if manager_args.kill:
        os.system("arckill -j multijobs.dat")
        return

    merger = HejPythiaMerger(args["user_name"], args["output_dir"])
    if manager_args.finalise:
        merger.copy_files()
        os.system("arcclean -j multijobs.dat")
        return

    if manager_args.merge:
        merger.merge_output()
        return
    

if __name__ == """__main__""":
    """
    Alter the values in 'args' to produce appropriate configurations:
        n_min      : int starting job number
        n_max      : int final job number
        events     : int number of events per run
        processes  : int number of runs per core < 4
        user_name  : user name on gridui and dpm storage
        base_dir   : base directory containing run configuration files
        rivet_dir  : directory containing rivet analyses
        output_dir : directory on grid storage for output, with protocol
        name : job name
    """

    args = {
           "n_min"      : 1,
           "n_max"      : 100,
           "events"     : 100000,
           "processes"  : 4,
           "user_name"  : "hhassan",
           "job_name"   : "/mt/home/hhassan/HEP-Tools/grid-methods/src/HejPythiaJob/run_hejpythia.py",
           "base_dir"   : "/mt/home/hhassan/Projects/HEJ_PYTHIA/pythia_merging/Setup/7TeV/7TeV-20GeV-R06/2j_HT2_7TeV/",
           "rivet_dir"  : "/mt/home/hhassan/Projects/HEJ_PYTHIA/pythia_merging/rivet",
           "output_dir" : "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/hhassan/pythia_merging/azimuthal-20GeV-2jet-single-run",
    }

    main(args)
