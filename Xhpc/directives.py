# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import sys
from os.path import abspath
from Xhpc.nodes import get_nodelist, allocate_nodes, get_nodes_ppn


def set_account(args: dict) -> str:
    """Get the directives for the user account.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            account : str
                Name of the account
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['account']:
        if args['torque']:
            directive = '#PBS -A %s' % args['account']
        else:
            directive = '#SBATCH --account=%s' % args['account']
        return directive


def set_partition(args: dict) -> str:
    """Get the directives for the allocation of a partition based on user input.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            partition: str
                Partition name
            gpu: bool
                Query a gpu (experimental)
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        directive = '#PBS -q %s' % args['partition']
    else:
        partition = '#SBATCH --partition=normal'
        if args['gpu']:
            partition = '#SBATCH --partition=accel'
        elif args['partition']:
            partition = '#SBATCH --partition=%s' % args['partition']
        directive = partition
    return directive


def set_environment(args: dict) -> str:
    """Get the directives for the user environment.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        directive = '#PBS -V'
    else:
        directive = '#SBATCH --export=ALL'
    return directive


def set_job(args: dict) -> str:
    """Get the directives for the user environment.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            job: str
                Job name
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        directive = '#PBS -N %s' % args['job']
    else:
        directive = '#SBATCH --job-name=%s' % args['job']
    return directive


def set_email(args: dict) -> str:
    """Get the directives for the emailing.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            email: bool
                Send email (always if fail)
            torque: bool
                Adapt to Torque
            email_address : str
                email address of the user

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        if args['email']:
            directive = '#PBS -m ae'
        else:
            directive = '#PBS -m a'
        directive += '\n#PBS -M %s' % args['email_address']
    else:
        if args['email']:
            directive = '#SBATCH --mail-type=END,FAIL,TIME_LIMIT_80'
        else:
            directive = '#SBATCH --mail-type=FAIL,TIME_LIMIT_80'
        directive += '\n#SBATCH --mail-user=%s' % args['email_address']
    return directive


def set_stdout_stderr(args: dict) -> str:
    """Get the directives for the working directory where the job stdout and
    stderr files will be written. The path (no extension) of the stdout and
    stderr files will also be collected.
    This results in extending the `args` dictionary with the "std_path" key
    pointing to this path as a value. The environment variable pointing to
    the job ID is also registered under key "job_id".

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            workdir: str
                Working directory
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        work_dir = '${PBS_O_WORKDIR}/${PBS_JOBNAME}'
        if args['workdir']:
            work_dir = '%s/${PBS_JOBNAME}' % abspath(args['workdir'])
        job_id = 'PBS_JOBID'
        std_path = 'localhost:%s_${%s}' % (work_dir, job_id)
        directive = '#PBS -o %s.o\n#PBS -e %s.e' % (std_path, std_path)
    else:
        job_id = 'SLURM_JOB_ID'
        std_path_ = 'slurm-%sx_%sj' % ('%', '%')
        std_path = 'slurm-${SLURM_JOB_NAME}_${SLURM_JOB_ID}'
        directive = '#SBATCH -o %s.o\n#SBATCH -e %s.e' % (std_path_, std_path_)
    args['std_path'] = std_path
    args['job_id'] = job_id
    return directive


def set_time(args: dict) -> str:
    """Get the directives for the time.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            time: str
                Wall time limit
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        directive = '#PBS -l walltime=%s:00:00' % args['time']
    else:
        directive = '#SBATCH --time=%s:00:00' % args['time']
    return directive


def set_memory(args: dict) -> str:
    """Get the directives for the memory.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            mem: tuple
                Requested memory
            mem_per_cpu: tuple
                Requested memory per cpu
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    for mem in ['mem_per_cpu', 'mem']:
        if not args[mem]:
            continue
        if args['torque']:
            directive = '#PBS -l mem=%s' % (''.join(args[mem])).lower()
        else:
            num, byt = args[mem]
            directive = '#SBATCH --%s=%s%s' % (
                mem.replace('_', '-'), num, byt.upper())
        return directive


def set_localscratch(args: dict) -> str:
    """Get the directives for the localscratch --gres option.
    Only available for the SAGA cluster and on Slurm.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            localscratch: tuple
                Use localscratch with the provided memory amount (in gb)
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if not args['torque'] and args['localscratch']:
        directive = '#SBATCH --gres=localscratch:%sG' % args['localscratch']
        args['localscratch'] = '/localscratch'
        return directive


def set_nodes(args: dict) -> str:
    """Get the directives for the nodes.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            nodes: tuple
                Node names
            torque: bool
                Adapt to Torque

    Returns
    -------
    directive : str
        Current directive
    """
    if args['nodes']:
        directive = get_nodelist(args)
    elif args['allocate']:
        directive = allocate_nodes(args)
    else:
        directive = get_nodes_ppn(args)
    return directive


def get_directives(args: dict) -> None:
    """Collect all the directives for the Torque or Slurm job.
    This results in extending the `args` dictionary with the "directives"
    key, pointing to the list of job directives.

    Parameters
    ----------
    args : dict
        All arguments
    """
    # init directives with a shebang that's hopefully universal here
    args['directives'] = ['#!/bin/bash']
    for set_directive in (
        set_environment,
        set_account,
        set_partition,
        set_job,
        set_localscratch,
        set_email,
        set_stdout_stderr,
        set_time,
        set_memory,
        set_nodes
    ):
        directive = set_directive(args)
        if directive:
            args['directives'].append(directive)
