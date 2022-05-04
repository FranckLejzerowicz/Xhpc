# Xhpc

Convert of .sh script into Torque's .pbs or Slurm's script for use on a 
computer cluster with intel or gpu nodes

## Description

Working on computer cluster necessitates the querying of nodes, 
cpus-per-node and memory resources that is conveniently done using 
directives in a script for job schedulers such as
[Slurm](https://slurm.schedmd.com/documentation.html) or
[Torque](http://docs.adaptivecomputing.com/torque/4-0-2/help.htm).

This package allows one to pass on a bash/sh script and get a copy of 
the script, but expanded with Slurm (or Torque) directives and other file 
system operations in order to get the scripts to run as requested and for it 
to take care of moving files to (local)scratch location for more efficiency. 

From the user is only needed an email address (to get notified if a job 
crashes or completes... and for nothing else!)

## Install

```
git clone https://github.com/FranckLejzerowicz/Xhpc.git
cd Xhpc
pip install -e .
```
*_Note that python should be python3_

## Requisite

**Attention**:

1. upon first use, the user is asked for and email address, that will be 
   written in `Xhpc/config.txt`. This file can be edited afterwards if you 
   want to change your email address.
2. please consider setting up a temporary directory using the environment 
   variable `$TMPDIR` (for example in your `.bash_profile` dotfile), if you 
   want the temporary files of some jobs to be written somewhere 
   convenient and removed before the job completes, e.g.:
  `export $TMPDIR="/Users/edith/temporary_dir""`.

## Input

Two types of inputs can be passed to `-i` or `--i-script`:
1. A path to a file containing sh/bash command line(s) (the file extension 
   needs not to be `.sh`)
2. A command directly (double quoted, e.g. `-i "tar xz archive.tar.gz folder"`) 

It might be of great convenience for you to use the option `-e` to give the 
name of a conda environment within which you have installed a software and 
that the job activate before running the script.  


## Outputs

A Slurm (or Torque) script including directives for resources querying, 
which by default have the extension `.slm` (or `.pbs`).

The user would then need to:
1. Check the output `.slm` (or `.pbs`) script for modified paths or errors 
   (**strongly** advised - this is tested, but I'm a human too).
    * especially if option `--scratch` or `--localscratch` is used (copies 
      input files and execute on the job's `$LOCALSCRATCH` folder), as in 
      this case some existing path may be copied that should not be (e.g. 
      the executables of piped programs) 
    * **attention**: if option `--run` is used, the output script will be 
      run immediately (use `--run` with caution!) 
2. Run `sbatch <outputname>.slm` (Slurm) or `qsub <outputname>.pbs` (for 
   Torque)


## Options

One key aspect of this package is that it takes care of moving to the 
scratch folder the existing input files and folders that are passed in 
command lines. This can be cumbersome to track and therefore, for this be 
done in the resulting `.slm` or `.pbs` script, one just need to set the 
´--move´ option.
By default, the files will be moved to the folder defined in the TMPDIR 
environment variable, but it is strongly advised to change this 
default location to a scratch folder, using at least one of:
* `--scratch`
* `--userscratch`
* `--localscratch`

If `--scratch` or `--userscratch` is activated for the first time, you will
be queried to enter the absolute paths to these locations. You can choose to 
enter a path or an environment variable. For example, on the NRIS's machine 
"SAGA", these two scratch locations already exist, under variables 
"SCRATCH" and "USERWORK" (feel free to input these variables instead).
[see here](https://documentation.sigma2.no/files_storage/clusters.html)
You can always change this path using option `--config-scratch`.

If `--localscratch` is used, then you just need to give an integer 
corresponding to an amount of memory (in GB) that will be allocated to the 
`--gres=localscratch:<MEM>` option (in Slurm).

Note that it is possible to prevent some files and folders to be moved to 
scratch bz providing their relative path *within* the input folder, using 
option `--exclude`. This can be very useful if you run a command that takes 
as input a folder that not only contains some the  files/folders you need, 
but also many, many other files/folders. For example, say you have such a 
folder called "input_dir" with some path in the command lines:
```
.
└── input_dir
    ├── useful.dat            # file in command lines
    ├── useful                # folder in command lines
    │   ├── useful_too.dat    # file in command lines
    │   ├── useless.dat
    │   ├── useless
    │   └── ...
    └── partly_useful
        ├── huge_useless
        │   └── ...
        └── useful_too        # folder in command lines
            └── ...
```
In there, you may only need to move:
* `./input_dir/useful.dat`
* `./input_dir/useful`
* `./input_dir/useful/useful_too.dat`
* `./input_dir/partly_useful/useful_too`

But the basic command to move these to scratch (using `rsync`) will include 
folder `./input_dir/useful`, and thus all what if contains. To avoid these 
useless files/folder, just specify using option `--exclude` *relative to the 
input folders*:
```
--exclude useless.dat --exclude useless --exclude partly_useful/huge_useless
```
Think of wildcards:
```
--exclude *useless* --exclude */*useless
```

## Usage

```
Xhpc -i </path/to/file.sh> -o </path/to/output.slm> -j <job_name> ... 
```

### Options
```
  -i, --i-script TEXT             Input script path (or double-quoted command)
                                  [required]
  -o, --o-script TEXT             Output script path (default to
                                  <input>_<YYYY-MM-DD-HH-MM-SS>.slm)
  -j, --i-job TEXT                Job name  [required]
  -a, --p-account TEXT            Name of the account
  -p, --p-partition [normal|bigmem|accel|optimist]
                                  Partition name
  -n, --p-nnodes INTEGER          Number of nodes  [default: 1]
  -c, --p-cpus INTEGER            Number of CPUs  [default: 1]
  -t, --p-time INTEGER            Wall time limit (in hours)  [default: 1]
  -M, --p-mem TEXT...             Requested memory as two space-separated
                                  entries: an integer and either 'MB' or 'GB'.
                                  (Default: '500 MB')
  -m, --p-mem-per-cpu TEXT...     Requested memory per cpu as two space-
                                  separated entries: an integer and either
                                  'MB' or 'GB'. (Default: '500 MB')
  -N, --p-nodes TEXT              Node names, e.g. `-N c1-4 -N c6-10 -N c7-1`
                                  (overrides option `-n`)
  -e, --p-env TEXT                Conda environment to run the job
  -d, --p-dir TEXT                Output directory  [default: .]
  -T, --p-tmp TEXT                Alternative temp folder to the one defined
                                  in $TMPDIR (if not defined: will be set to
                                  $USERWORK, or to $SCRATCH if any scratch
                                  option is activated)
  -w, --p-workdir TEXT            Working directory to use instead of
                                  $SLURM_SUBMIT_DIR
  -y, --p-include TEXT            Folder to not move to and from scratch using
                                  rsync (must exist)
  -x, --p-exclude TEXT            Relative path(s) within input folder(s) to
                                  not move in scratch
  --move / --no-move              Move files/folders to chosen scratch
                                  location  [default: no-move]
  -l, --localscratch INTEGER      Use localscratch with the provided memory
                                  amount (in GB)
  --scratch / --no-scratch        Use the scratch folder to move files and
                                  compute  [default: no-scratch]
  --userscratch / --no-userscratch
                                  Use the userscratch folder to move files and
                                  compute  [default: no-userscratch]
  --clear-scratch / --no-clear-scratch
                                  Whether to cleat the scratch area at the end
                                  of the job or not  [default: clear-scratch]
  --stdout / --no-stdout          Rename stdout (and stderr) with job name and
                                  ID  [default: stdout]
  --email / --no-email            Send email at job completion (always if
                                  fail)  [default: no-email]
  --run / --no-run                Run the job before exiting (subprocess)
                                  [default: no-run]
  --stat / --no-stat              Whether to prepend `/usr/bin/time -v` to
                                  every script command  [default: stat]
  --verif / --no-verif            Print script to stdout and ask for 'y/n'
                                  user input to sanity check  [default: no-
                                  verif]
  --gpu / --no-gpu                Query a gpu (experimental)  [default: no-
                                  gpu]
  --torque / --no-torque          Adapt to Torque  [default: no-torque]
  --config-email / --no-config-email
                                  Show current email and/or edit it  [default:
                                  no-config-email]
  --config-scratch / --no-config-scratch
                                  Show current scratch folders and/or edit
                                  them  [default: no-config-scratch]
  --sinfo / --no-sinfo            Print sinfo in stdout (and update
                                  `~/.sinfo`)  [default: no-sinfo]
  --allocate / --no-allocate      Get current machine usage (sinfo) to
                                  allocate suitable nodes/memory  [default:
                                  no-allocate]
  --version                       Show the version and exit.
  --help                          Show this message and exit.
```

### Bug Reports

contact `franck.lejzerowicz@gmail.com`

### Acknowledgments

Tomasz Kosciolek had been the one providing me with a first example of PBS
script that collect nice job info (temp path, setup) - Thanks Tomek!
