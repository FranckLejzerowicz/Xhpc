# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os
import sys
import glob
import subprocess
import pandas as pd
from datetime import datetime
from os.path import abspath, isfile, isdir


def init_args(args: dict) -> None:
    """Initialize key/value pairs in the args dict to be filled is necessary.

    Parameters
    ----------
    args : dict
        All arguments
    """
    args['scratching'] = []
    args['mkdir'] = set()
    args['move_to'] = set()
    args['move_from'] = set()
    args['clear'] = []
    if not args['include']:
        args['include'] = ()
    if not args['exclude']:
        args['exclude'] = ()


def get_sinfo_pd(args: dict, sinfo_dir: str) -> None:
    """Collect the nodes info from the output of sinfo if the user asks
    for Xhpc to allocate specific nodes ot the job being created,
    or if the user wishes to update sinfo (it is run once per day by default
    but can be called again if one needs to use nodes that may become
    available in the course of the day).
    The `args` dictionary will be extended with a "sinfo_pd" key, pointing
    to a pd.DataFrame containing the påer-node sinfo output for an
    extensive set of parameters.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            sinfo: bool
                Whether to update node usage info (overwrites `~/.sinfo` too)
            allocate : bool
                Get current machine usage to allocate suitable nodes/memory
            torque : bool
                Switch from Slurm to Torque
    sinfo_dir : str
        Path to the .sinfo folder where is the most up-to-date sinfo file
    """
    sinfo_pd = pd.DataFrame()
    # # -------------------------------------------------------------
    # # -----------               DELETE                  -----------
    # # -------------------------------------------------------------
    # fp = '/Users/franck/programs/Xhpc/Xhpc/test/snap.txt'
    # sinfo_pd = pd.read_csv(fp, sep='\t')
    # args['sinfo_pd'] = sinfo_pd
    # return sinfo_pd
    # # -------------------------------------------------------------
    # # -------------------------------------------------------------
    if args['torque']:
        raise IOError('No node allocation yet avail for PBS/Torque')
    # if the user wants to update sinfo or needs to Xhpc to
    # allocate specific nodes/cores for the job.
    elif args['sinfo'] or args['allocate']:
        # then we need to rely on the latest sinfo data, which is located in
        # the dot folder '.sinfo' and in the file time stamped for today

        # -------------------------------------------------------------
        # -----------               CHANGE                  -----------
        # -------------------------------------------------------------
        # fp = '/Users/franck/programs/Xhpc/Xhpc/test/snap.txt'
        fp = '%s/%s.tsv' % (sinfo_dir, str(datetime.now().date()))
        # -------------------------------------------------------------
        # -------------------------------------------------------------

        # if there is no file today or if the user wants to update sinfo
        if args['sinfo'] or not isfile(fp):
            # create the dot folder '.sinfo' if needed
            if not isdir(sinfo_dir):
                os.makedirs(sinfo_dir)
            # collect the sinfo data
            sinfo_pd = collect_sinfo_pd()
            # remove previous files for former days
            remove_previous_fps(sinfo_dir)
            # write it out as the latest, today's file
            sinfo_pd.to_csv(fp, index=False, sep='\t')
        # if there is a file already for today
        else:
            # just read it in!
            sinfo_pd = pd.read_csv(fp, sep='\t')
    args['sinfo_pd'] = sinfo_pd


def decode_sinfo_stdout(sinfo_stdout: bytes) -> list:
    """Transform the raw bytes text sinfo stdout output into rows represented
    as  lists (of lists).

    Parameters
    ----------
    sinfo_stdout : bytes
        per node sinfo subprocess stdout for an extensive set of parameters

    Returns
    -------
    sinfo_list : list of lists
        Per-node sinfo output for an extensive set of parameters
    """
    sinfo_decoded = sinfo_stdout.decode()
    sinfo_split = sinfo_decoded.strip().split('\n')
    sinfo_list = [row.split() for row in sinfo_split]
    return sinfo_list


def collect_sinfo_pd() -> pd.DataFrame:
    """Run sinfo as a subprocess and collect the sinfo subprocess results as
    a pandas table.

    Returns
    -------
    sinfo_pd : pd.DataFrame
        per node sinfo subprocess stdout for an extensive set of  parameters
    """
    sinfo_stdout = run_sinfo_subprocess()
    sinfo_out = decode_sinfo_stdout(sinfo_stdout)
    sinfo_pd = pd.DataFrame(sinfo_out, columns=[
        'node', 'partition', 'status', 'cpu_load', 'cpus',
        'socket', 'cores', 'threads', 'mem', 'free_mem'])
    return sinfo_pd


def run_sinfo_subprocess() -> bytes:
    """Run sinfo as a subprocess.

    Returns
    -------
    sinfo_stdout : bytes
        per node sinfo subprocess stdout for an extensive set of  parameters
    """
    fmt = 'NodeList:10,'
    fmt += 'Partition:10,'
    fmt += 'StateLong:10,'
    fmt += 'CPUsLoad:10,'
    fmt += 'CPUsState:12,'
    fmt += 'Sockets:4,'
    fmt += 'Cores:4,'
    fmt += 'Threads:4,'
    fmt += 'Memory:12,'
    fmt += 'FreeMem:12'
    cmd = ['sinfo', '--Node', '-h', '-O', fmt]
    # run sinfo and get the stdout
    sinfo_stdout, stderr = subprocess.Popen(
        ' '.join(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True
    ).communicate()
    if stderr:
        raise FileNotFoundError('Using Slurm? `sinfo` command not found')
    return sinfo_stdout


def remove_previous_fps(sinfo_dir: str) -> None:
    """Clear the .sinfo folder from every already-existsing / previous sinfo
    file in order to only keep the most up-to-date sinfo data (written after
    this function is called).

    Parameters
    ----------
    sinfo_dir : str
        Path to the .sinfo folder where is the most up-to-date sinfo file
    """
    for previous_fp in glob.glob('%s/*.tsv' % sinfo_dir):
        os.remove(previous_fp)


def get_job_fp(args: dict) -> None:
    """Get the path to the file that will be written to contains all the
    slurm (or torque) job directives and the job commands it self.
    This results in extending the `args` dictionary with the "job_fp"
    key, pointing to the path to the job file. By default, this file is the
    input extended with date and time info and `.slm` for slurm (or `.pbs`
    for torque).

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            job: str
                Job name
            output_fp: str
                Output script path
            torque: bool
                Adapt to Torque
    """
    # if the user did not specify a path to a script output file using option
    # `--o-script`, create one based on the input and the date
    if not args['output_fp']:
        # format the date and time of now
        now_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # add `.slm` (or `.pbs`) to a concatenation of path, job name and time
        job_rad = '%s/%s_%s' % (abspath('.'), args['job'], now_time)
        if args['torque']:
            args['job_fp'] = '%s.pbs' % job_rad
        else:
            args['job_fp'] = '%s.slm' % job_rad
    # if the output filename was provided, just use it
    else:
        args['job_fp'] = args['output_fp']


def get_output_dir(args: dict) -> None:
    """Get the absolute path of the output directory.
    This results in extending the `args` dictionary with the "output_dir"
    key, pointing to the output directory (full path) to use for the job.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
        out_dir: str
            Output directory
    """
    out_dir = args['out_dir']
    if isdir(out_dir):
        args['output_dir'] = abspath(out_dir)
    else:
        args['output_dir'] = abspath('.')


def get_first_line(path: str) -> str:
    """Get the first line of a file.

    Parameters
    ----------
    path : str
        Path to a file

    Returns
    -------
    ret: str
        First line of the file
    """
    ret = ""
    with open(path) as f:
        for line in f:
            ret = line.strip()
            break
    return ret


def get_tmpdir(args: dict) -> bool:
    """Define the path to a temporary folder depending on the user options
    such as the use of scratch or localscratch, or on the presence of an
    already existing TMPDIR folder, or on whether the user specified a tmp
    folder to use. It creates the folder and export an environment variable
    TMPDIR for it.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            tmp: str
                Alternative temp folder
            move : bool
                Move files/folders to chosen scratch location
            torque: bool
                Adapt to Torque
    """
    # define the temporary folder
    if args['tmp']:
        tmpdir = args['tmp'].rstrip('/')
    elif args['move']:
        # if this is set, then there will be a SCRATCH_FOLDER folder created
        tmpdir = '${SCRATCH_FOLDER}/tmpdir'
    elif 'TMPDIR' in os.environ:
        tmpdir = '${TMPDIR}'
    else:
        return None  # if no TMPDIR is set, keep the default machine behavior

    tmpdir += '_' + args['job']
    if args['torque']:
        tmpdir += '_${PBS_JOBID}'
    else:
        tmpdir += '_${SLURM_JOB_ID}'

    # set command to create the temporary folder
    args['tmp'] = [
        '# create and export the temporary directory',
        'mkdir -p %s' % tmpdir,
        'export TMPDIR="%s"' % tmpdir,
        'echo Temporary directory is ${TMPDIR}']
    args['clear'].append('rm -rf ${TMPDIR}')


def check_content(args: dict) -> None:
    """Ask user to check for the job script content, which is composed of
    "directives", "preamble", and "commands"

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            verif: bool
                Print script to stdout and query user for sanity
            directives : list
                Commands composing the job's directives
            preamble : list
                Commands composing the job's preamble
            commands : list
                Commands composing the actual job
    """
    if args['verif']:
        for part in ['directives', 'preamble', 'scratching', 'tmp', 'mkdir',
                     'move_to', 'commands', 'move_from', 'clear']:
            if part not in args or not args[part]:
                continue
            print('------------------------%s' % ('-' * len(part)))
            print("Please check the job's %s:" % part)
            print('------------------------%s' % ('-' * len(part)))
            for command in args[part]:
                print(command)
            ret = input('\n\nContinue to write?: <[y]/n>\n')
            if ret == 'n':
                print('Exiting\n')
                sys.exit(1)


def get_lines(part) -> list:
    """

    Parameters
    ----------
    part : list or set
        Commands to write out for a section

    Returns
    -------
    lines : list
        Commands list to write out for a section (if was a set, a sorted list)
    """
    if isinstance(part, list):
        lines = part
    elif isinstance(part, set):
        lines = sorted(part)
    else:
        raise TypeError('Commands are collected either as list or set')
    return lines


def write_out(args: dict) -> None:
    """Write the actual .pbs / slurm .sh script based on
    the info collected from the command line.

    Parameters
    ----------
    args : dict
        All arguments, including:
            directives : list
                Commands composing the job's directives
            preamble : list
                Commands composing the job's preamble
            commands : list
                Commands composing the actual job
            stat: bool
                Whether to prepend `/usr/bin/time -v` to every script command
            quiet : bool
                Do not print anything
    """
    with open(args['job_fp'], 'w') as o:
        for part in ['directives', 'preamble', 'scratching', 'tmp', 'mkdir',
                     'move_to', 'commands', 'move_from', 'clear']:
            if part not in args or not args[part]:
                continue
            next_line = False
            lines = get_lines(args[part])
            for line_ in lines:
                line = line_
                if args['stat'] and part == 'commands':
                    if next_line or not line_.strip():
                        line = line_
                    else:
                        line = '/usr/bin/time -v %s' % line_
                o.write('%s\n' % line)
                next_line = False
                if line.endswith('\\'):
                    next_line = True
            if part in args:
                o.write('# ------ %s END ------\n' % part)
            o.write('\n')
        o.write('echo "Done!"\n')
        if not args['quiet']:
            print('Written:', args['job_fp'])


def show_config(args: dict, config_fp: str) -> None:
    """

    Parameters
    ----------
    args : dict
        All arguments. Here only the following key is of interest:
        scratch: str
            Path to scratch folder (to move files and compute)
        userscratch: str
            Path to user scratch folder (to move files and compute)
        email_address : str
            email address of the user.
    config_fp : str
        Path to the file that may (or not) contains an email address
    """
    if isfile(config_fp):
        print('* email address config file:',
              subprocess.getoutput('cat %s' % config_fp))
    else:
        print('Email address config not yet created')
        sys.exit(1)

    for scratch in ['scratch', 'userscratch']:
        if args[scratch]:
            scratch_fp = config_fp.replace('config.txt', '%s.txt' % scratch)
            if isfile(scratch_fp):
                print('* %s folder config file:' % scratch,
                      subprocess.getoutput('cat %s' % scratch_fp))
            else:
                print('%s config not created (run Xhpc using `--%s`)' % (
                    scratch, scratch))
                sys.exit(1)


def sys_exit(args: dict):
    """Quit if some arguments are not passed by user.
    Note that these could be set using required=True in the click options,
    but this is not suited to letting this tools reach the show_config().

    Parameters
    ----------
    args : dict
        All arguments, including:
            input_fp : str
                Input script path (or double-quoted command)
            job: str
                Job name
    """
    if not args['input_fp']:
        print('Option "-i" (or "--i-script") is mandatory')
        sys.exit(0)
    if not args['job']:
        print('Option "-j" (or "--i-job") is mandatory')
        sys.exit(0)
