# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import glob
from os.path import basename


def get_conda_path(env: str) -> str:
    """List the executables by parsing the binaries in the bin folder of the
    conda environment. Sometimes, the envs are installed somewhere as set
    out in some env variables. Hence, this tool follows the order of
    precedence of the conda: first will look into CONDA_ENVS_PATH, then will
    look relative to CONDA_EXE and finally, will find using a subprocess. A
    subprocess can take a few seconds to run, so I recommend the user set a
    CONDA_ENVS_PATH variable (e.g., in the `.bashrc` file as explained here:
    https://docs.conda.io/projects/conda/en/latest/user-guide/configuration
    /use-condarc.html#specify-environment-directories-envs-dirs).

    Parameters
    ----------
    env : str
        Conda environment to run the job

    Raises
    ------
    OSError
        If the given env is not an absolute path, or that no useful env
        variable exists for conda that can be used to fond the env binaries.

    Returns
    -------
    path : str
        Path to the set of executables from the conda environment
    """
    path = None
    if env:
        if env[0] == '/':
            path = env
        elif 'CONDA_ENVS_PATH' in os.environ:
            path = '%s/%s' % (os.environ['CONDA_ENVS_PATH'], env)
        elif 'CONDA_EXE' in os.environ:
            path = os.environ['CONDA_EXE'].replace('bin/conda', 'envs/%s' % env)
        else:
            raise OSError('Make sure conda is installed')
    return path


def get_conda_executables(args: dict) -> set:
    """Get the basename of all executables present in the bin folder of the
    conda environment that the job will run in, in order for these
    executables not be affected by scratch/localscratch transfers.

    Parameters
    ----------
    args : dict
        All arguments
        env : str
            Conda environment to run the job

    Returns
    -------
    conda_executables : set
        Set of executables
    """
    conda_executables = set()
    if args['env']:
        args['env'] = get_conda_path(args['env'])
        conda_executables.update(
            [basename(x) for x in glob.glob('%s/bin/*' % args['env'])])
    return conda_executables


def get_path_executables() -> set:
    """Collect the set of files that are located in the paths listed in every
    environment variable containing "PATH". That should include "PATH",
    "PYTHONPATH", and other paths set specifically and that may contains
    files for tools that are ok if moved around (e.g. "GTDBTK_DATA_PATH").
    Indeed, this collected set is used later to avoid changing moving
    binaries from and to the (local)scratch locations.

    Returns
    -------
    path_executables : set
        Path to the set of executables from the user's environment
    """
    path_executables = set()
    for env_variable, env_variable_value in os.environ.items():
        if 'PATH' in env_variable:
            for path in env_variable_value.split(':'):
                path_executables.update(
                    [basename(x) for x in glob.glob('%s/*' % path)])
    return path_executables


def get_executables(args: dict) -> None:
    """Get the basename of all executables present in the bin folder of the
    conda environment that the job will run in and of every environment
    variable containing "PATH". That should include "PATH", "PYTHONPATH",
    and other paths set specifically and that may contains files for tools
    that are ok if moved around (e.g. "GTDBTK_DATA_PATH").
    Indeed, this collected set is used later to avoid changing moving
    binaries from and to the (local)scratch locations, in order for these
    executables not be affected by scratch/localscratch transfers.
    This results in extending the `args` dictionary with the "executables"
    key, pointing to the set of conda and global paths executables.

    Parameters
    ----------
    args : dict
        All arguments
    """
    executables = get_path_executables()
    executables.update(get_conda_executables(args))
    args['executables'] = executables
