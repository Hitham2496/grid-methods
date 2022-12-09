
# HEP Grid Methods

This repository contains scripts adapted to submit HEP software jobs on the Durham ARC Grid High Performance Computing system.

The scripts are based on tools originally written by Jeppe Andersen and Tuomas Hapola for [Sherpa](https://sherpa-team.gitlab.io/) in python 2.

The current scripts include methods for running HEJ+Pythia (under development) and [HEJ](https://hej.hepforge.org/) and the format is less than ideal since compatibility with python 2 is required for use on the Grid (for instance I would normally use python 3 [f-strings](https://docs.python.org/3/tutorial/inputoutput.html) rather than %s to include variable values in strings).

The original scripts made use of `os.system()` rather than `subprocess.Popen()` which uses non-blocking commands and so would have been preferrable.

To retain functionality in porting the prior scripts which were used to produce data for publications the implementation with `os.system()` was kept here.

Additionally, producing a python package for this submission system is made difficult by the out-of-date build tools on the grid meaning installing packages to one's local environment is significantly more difficult.

## Format
Example job script templates may be found in `src/JobTemplate`, we here guide the user through use of the HEJ+Pythia scripts in `src/HejPythiaJob`.

The scripts are split into a manager (`hejpythia_manager.py`) and a run script (`run_hejpythia.py`) which will run multiple jobs (up to a maximum of four) per submission node.

Output from each job is the [yoda](https://yoda.hepforge.org/) analysis file for each run (and scale variations) as well as the HEJ, Sherpa and HEJ+Pythia runcards used for the run (for debugging purposes).

The `HejPythiaMerger` class in `run_hejpythia.py` is used to copy and merge the output yoda files with the option to prune for significant outliers.

If pruning is enabled ensure your pruning tools are compiled and may be found in `$PATH`.

## Usage
The main script for submission and job management (`hejpythia_manager`) wraps around these tools and must be modified before running (in a manner similar to pyHepGrid runcards).

The modifications are all in the `args` dictionary parameter in `main()`, no other user modification should be required; alter the values for the appropriate run:

 - `n_min` : int starting job number
 - `n_max` : int final job number
 - `events` : int number of events per run
 - `processes` : int number of runs per submission < 4
 - `user_name` : user name on gridui and dpm storage
 - `base_dir` : base directory containing run configuration files
 - `rivet_dir` : directory containing rivet analyses
 - `output_dir` : directory on grid storage for output, including protocol
 - `name` : job name (i.e. name of the script to be run on each node, including path)

Then one needs only run the script with:
```
python hejpythia_manager.py -r
```
To interact with the job database (written to `$PWD/multijobs.dat`) one may use the standard [arc](https://www.ippp.dur.ac.uk/~andersen/GridTutorial/arc.html) tools, a wrapper around `arcstat` is provided with the manager script:
```
python hejpythia_manager.py -s
```
After concluding the run one may supply the `--finalise` or `-f` flag to the manager to copy the output files to a temporary directory in `/scratch/user_name/`, i.e.
```
python hejpythia_manager.py -f
```
The results may then be merged (and pruned if desired) by supplying the `--merge` or `-m` flag:
```
python hejpythia_manager.py -m
```
which writes the merged analysis output to `$PWD/results/merged`, in the future this method will also write the merged seeds to a log file.

## Recommendations

Since the path to the run methods is supplied to the job manager we recommend storing the run methods and base classes in a clearly-labelled directory and using the submission manager wherever it may be needed.

It is recommended to split runs up by jet multiplicity (which HEJ requires anyway) and to use a custom job manager in each directory.

While this is not the most efficient way, it does allow one to be certain of which parameters were used in the running while debugging (by simply checking the corresponding runcard).

No functionality is provided for combining merged yoda analysis output between jet multiplicities/ different runs since this is often analysis-specific, the user should construct their own methods or use standard tools (e.g. `yodamerge`, `yodastack`, ...) for the analysis they require.

## Custom Jobs

It is recommended to write methods based on the template files in `src/JobTemplate` for your executables.

Feel free to open a pull request for your own jobs :)
