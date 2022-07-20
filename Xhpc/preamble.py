# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
from os.path import abspath


def add_env(args: dict) -> None:
    """Add the preamble for the job.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            env : str
                Conda environment to run the job
    """
    args['preamble'] = [
        '# general environment info / behaviour',
        'uname -a',  # Show some machine specs for reproducibility
        'set -e',    # Exit the script on any error
    ]
    # if a conda environment is used
    if args['env']:
        # activate the env
        env = ['\n# active conda environment',
               'echo "Conda environment is %s"' % args['env']]
        if args['saga']:
            env.extend([
                'module load Anaconda3/2019.03',
                'export PS1=\\$',
                'source ${EBROOTANACONDA3}/etc/profile.d/conda.sh',
                'conda deactivate &>/dev/null'])
        env.append('conda activate %s' % args['env'])
        if args['saga']:
            env.append('module purge')
        args['preamble'].extend(env)


def add_workdir(args: dict) -> None:
    """Add the job's environment variable setting the working directory.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            workdir: str
                Working directory
            torque: bool
                Adapt to Torque
    """
    if args['workdir']:
        export = "export SLURM_SUBMIT_DIR='%s'" % args['workdir']
        if args['torque']:
            export = "export PBS_O_WORKDIR='%s'" % args['workdir']
        args['preamble'].extend(['\n# change working directory', export])


def add_tmpdir(args: dict) -> bool:
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
            workdir: str
                Working directory
            move : bool
                Move files/folders to chosen scratch location
            torque: bool
                Adapt to Torque
    Returns
    -------
    tmp : bool
        Whether a temporary folder is set or not
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
        return False  # if no TMPDIR is set, keep the default machine behavior

    tmpdir += '/' + args['job']
    if args['torque']:
        tmpdir += '_${PBS_JOBID}'
    else:
        tmpdir += '_${SLURM_JOB_ID}'

    # set command to create the temporary folder
    args['preamble'].extend([
        '# create and export the temporary directory',
        'mkdir -p %s' % tmpdir,
        'export TMPDIR="%s"' % tmpdir,
        'echo Temporary directory is ${TMPDIR}'
    ])
    args['clear'].append('rm -rf ${TMPDIR}')
    return True


def add_procs_nodes(args: dict) -> None:
    """Get the number of nodes and processors actually used by the job.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            torque: bool
                Adapt to Torque
    """
    # Calculate the number of processors/nodes allocated to this run.
    if args['torque']:
        args['preamble'].append('NPROCS=`wc -l < ${PBS_NODEFILE}`')
        args['preamble'].append('NNODES=`uniq ${PBS_NODEFILE} | wc -l`')
    else:
        args['preamble'].append('NPROCS=${SLURM_NTASKS}')
        args['preamble'].append('NNODES=${SLURM_NNODES}')
    args['preamble'].append('echo Use ${NPROCS} procs on ${NNODES} nodes')


def add_echoes(args: dict) -> None:
    """Add things to echo in the job preamble that will be available in the
    stdout.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            input_fp : str
                Input script path (or double-quoted command)
            workdir: str
                Working directory
            torque: bool
                Adapt to Torque
    """
    args['preamble'].extend([
        '\n# echo some info about the job',
        'echo Running on host `hostname`',
        'echo Time is `date`',
        'echo Directory is `pwd`'
    ])
    add_procs_nodes(args)
    stdout = 'echo Job stdout is %s' % args['std_path'],
    stderr = 'echo Job stderr is %s' % args['std_path']
    args['preamble'].extend(['%s.o' % stdout, '%s.e' % stderr])
    args['preamble'].append('echo Job script: %s' % abspath(args['input_fp']))


def get_preamble(args: dict) -> None:
    """Get lines to be written as preamble to the job (not the directives):
    - load conda environment for the job
    - setup temporary directory
    - echo important things for the stdout file

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            torque: bool
                Adapt to Torque
    """
    args['preamble'] = list()
    add_env(args)  # environment variables
    # set an alternative workdir for the job
    add_workdir(args)
    # set preamble on the job environment
    add_echoes(args)
