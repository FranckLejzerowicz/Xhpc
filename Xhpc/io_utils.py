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
    # import yaml
    # fp = '/Users/franck/programs/Xhpc/Xhpc/test/snap.txt'
    # return pd.read_csv(fp, sep='\t')
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
        fp = '/Users/franck/programs/Xhpc/Xhpc/test/snap.txt'
        # fp = '%s/%s.tsv' % (sinfo_dir, str(datetime.now().date()))
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
    # with open('/Users/franck/programs/Xsinfo/Xsinfo/test/snap.txt') as o:
    #     sinfo_out = yaml.load(o, Loader=yaml.Loader)
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
    print(' '.join(cmd))
    # run sinfo and get the stdout
    sinfo_stdout, stderr = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
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
        for section in ['directives', 'preamble', 'commands']:
            print('------------------------%s' % ('-' * len(section)))
            print("Please check the job's %s:" % section)
            print('------------------------%s' % ('-' * len(section)))
            for command in args[section]:
                print(command)
            ret = input('\n\nContinue to write?: <[y]/n>\n')
            if ret == 'n':
                print('Exiting\n')
                sys.exit(1)


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
    """
    with open(args['job_fp'], 'w') as o:
        for part in ['directives', 'preamble', 'in', 'commands', 'mv', 'out']:
            for line in args[part]:
                o.write('%s\n' % line)
            o.write('# ------ %s END ------\n' % part)
            o.write('\n')
        o.write('echo "Done!"\n')