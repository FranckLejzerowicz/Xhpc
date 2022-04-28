# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import itertools
from os.path import dirname, isdir, isfile


def get_scratch_path(args: dict) -> str:
    """Get the path to the scratch folder to use to move and compute.
    If the use activate more than one of the three scratch arguments, then the
    scratch path to be used is in order of top to bottom priority, i.e.:
    - localscratch (fastest file system because on the running node)
    - scratch  (automatically created for the job, and then deleted)
    - userscratch (always exists and thus can be looked at, but not backed-up)

    Parameters
    ----------
    args : dict
        All arguments, including:
            scratch: str
                Path to scratch folder or None if not set
            userscratch: str
                Path to user scratch folder or None if not set
            localscratch: tuple
                Path "/localscratch" or command None (default) if not set

    Returns
    -------
    scratch_val : str
        Value for the scratch key folder
    """
    for scratch_path in ['localscratch', 'scratch', 'userscratch']:
        if args[scratch_path]:
            scratch_val = args[scratch_path]
            return scratch_val


def get_scratching_commands(args: dict) -> None:
    """Get the scratch folder creation and deletion commands.

    Parameters
    ----------
    args : dict
        All arguments, including:
            scratch: str
                Path to scratch folder or None if not set
            userscratch: str
                Path to user scratch folder or None if not set
            localscratch: tuple
                Path "/localscratch" or command None (default) if not set
            torque: bool
                Adapt to Torque
    """
    if args['scratch'] or args['userscratch'] or args['localscratch']:
        # Define scratch directory
        scratch_path = '%s/${%s}' % (get_scratch_path(args), args['job_id'])

        # Get commands to create and more to scratch directory
        args['in'].append('\n# Define and create a scratch directory')
        args['in'].append('SCRATCH_DIR="%s"' % scratch_path)
        args['in'].append('mkdir -p ${SCRATCH_DIR}')
        args['in'].append('cd ${SCRATCH_DIR}')
        args['in'].append('echo Working directory is ${SCRATCH_DIR}')

        # Get commands to move away and delete scratch directory
        args['out'].append('\n# Move away and clear out the scratch directory')
        if args['torque']:
            args['out'].append('cd ${PBS_O_WORKDIR}')
            args['out'].append('rm -rf ${SCRATCH_DIR}')
        else:
            args['out'].append('cd ${SLURM_SUBMIT_DIR}')
            args['out'].append('rm -rf ${SCRATCH_DIR}')
    else:
        args['in'].append('\n# Define scratch directory as the working dir')
        if args['torque']:
            args['in'].append('SCRATCH_DIR=$PBS_O_WORKDIR')
        else:
            args['in'].append('SCRATCH_DIR=$SLURM_SUBMIT_DIR')


def get_in_out(paths: set) -> dict:
    """Get the paths that would need to be move to/from scratch locations.

    Parameters
    ----------
    paths : set
        Paths to file or folders to move when using scratch folder

    Returns
    -------
    in_out : dict
        Sets of existing files and folder that will be moved in and
        of non-existing paths that will be move back.
    """
    in_out = {'files': set(), 'folders': set(), 'out': set()}
    for path in paths:
        if isfile(path):
            in_out['files'].add(path)
        elif isdir(path):
            in_out['folders'].add(path)
        else:
            in_out['out'].add(path)
    return in_out


def get_min_files(in_out: dict, min_folders: set) -> set:
    """Reduce the set of files to those not already within the folders.

    Parameters
    ----------
    in_out : dict
        Sets of existing files and folder that will be moved in and
        of non-existing paths that will be move back.
    min_folders : set
        Keep the most basal of the existing folders passed in command

    Returns
    -------
    min_files : set
        Files not present with the minimum set of folders
    """
    min_files = set()
    for f1 in in_out['files']:
        for f2 in min_folders:
            if f2 in dirname(f1):
                break
        else:
            min_files.add(f1)
    return min_files


def get_min_folders(in_out: dict) -> set:
    """Reduce the set of folder to that which is most basal so that

    Parameters
    ----------
    in_out : dict
        Sets of existing files and folder that will be moved in and
        of non-existing paths that will be move back.

    Returns
    -------
    min_folders : set
        Minimum set of existing folders to contain all folders passed in command
    """
    folders = in_out['folders']
    min_folders, exclude = set(), set()
    for f1, f2 in itertools.combinations(sorted(folders), 2):
        if f1 != f2 and f1 in f2:
            exclude.add(f2)
    min_folders = folders.difference(exclude)
    return min_folders


def get_exclude(args: dict) -> str:
    """Get the "--exclude" option of the rsync command to exclude files
    and/or folder provided b the user.

    Parameters
    ----------
    args : dict
        All arguments, including:
            exclude : tuple
                Relative path(s) within input folder(s) to not move in scratch

    Returns
    -------
    exclude : str
        Exclude command with the requested file and folder paths
    """
    exclude = ''
    if args['exclude']:
        # the leading "/" is because the previous term will be a folder, so
        # that the rsync command can work for folder contents transfer
        exclude = "/ --exclude={'%s'}" % "','".join(args['exclude'])
    return exclude


def move_in(path: str, is_folder: bool, exclude: str = '') -> list:
    """

    Parameters
    ----------
    path : str
        A folder or a file to move to scratch
    is_folder : bool
        Whether the path is a folder (True) or a file (False)
    exclude : str
        Exclude command with the requested file and folder paths

    Returns
    -------
    move : list
        Command to move a folder or a file to scratch
    """
    source = path
    if is_folder:
        source += '/'
    destination = '${SCRATCH_DIR}%s' % path
    move = [
        'mkdir -p %s' % dirname(destination),
        'rsync -aqru %s %s%s' % (source, destination, exclude)
    ]
    return move


def get_min_paths(in_out: dict) -> dict:
    """Get the minimum set of folders and files to move to scratch.

    Parameters
    ----------
    in_out : dict
        Sets of existing files and folder that will be moved in and
        of non-existing paths that will be move back.

    Returns
    -------
    min_paths : dict
        Minimum set of existing folders to contain all folders passed in
        command (key "folders") and files not present with the minimum set of
        folders (key "files")
    """
    min_folders = get_min_folders(in_out)
    min_files = get_min_files(in_out, min_folders)
    min_paths = {'folders': min_folders, 'files': min_files}
    return min_paths


def get_in_commands(args: dict, min_paths: dict, exclude: str = '') -> None:
    """Get command that move the inputs to scratch.

    Parameters
    ----------
    args : dict
        All arguments.
    min_paths : set
        Folder and files that must be move to the scratch folder
    exclude : str
        Exclude command with the requested file and folder paths
    """
    # folders
    for min_folder in min_paths['folders']:
        args['in'].extend(move_in(min_folder, True, exclude))
    # files
    for min_file in min_paths['files']:
        args['in'].extend(move_in(min_file, False))


def get_out_commands(args: dict, min_paths: dict, in_out: dict) -> None:
    """Get command that move the inputs in and outputs out.

    Parameters
    ----------
    args : dict
        All arguments.
    min_paths : dict
        Minimum set of existing folders to contain all folders passed in
        command (key "folders") and files not present with the minimum set of
        folders (key "files")
    in_out : dict
        Sets of existing files and folder that will be moved in and
        of non-existing paths that will be move back.
    """
    move = list()
    for folder in min_paths['folders']:
        source = '${SCRATCH_DIR}%s' % folder
        move.extend([
            'mkdir -p %s' % folder,
            'rsync -auqr %s/ %s; fi' % (source, folder)
        ])

    for path in in_out['out']:
        source = '${SCRATCH_DIR}%s' % path
        move.append(
            'if [ -d %s ]; then mkdir -p %s; rsync -auqr %s/ %s; fi' % (
                source, path, source, path))
        move.append(
            'if [ -f %s ]; then rsync -auqr %s %s; fi' % (
                source, source, path))
    args['mv'] = move


def get_relocating_commands(args: dict) -> None:
    """Get command that move the inputs in and outputs out.

    Parameters
    ----------
    args : dict
        All arguments, including:
            paths : set
                Paths to file or folders to move when using scratch folder
    """
    # Move in to scratch
    exclude = get_exclude(args)
    in_out = get_in_out(args['paths'])
    min_paths = get_min_paths(in_out)
    get_in_commands(args, min_paths, exclude)
    # Move out from scratch
    get_out_commands(args, min_paths, in_out)


def go_to_work(args: dict) -> None:
    """Get the working folder to go to and echo.

    Parameters
    ----------
    args : dict
        All arguments, including:
            torque: bool
                Adapt to Torque
    """
    args['in'] = ['# Move to the working directory and say it']
    work_dir = 'SLURM_SUBMIT_DIR'
    if args['torque']:
        work_dir = 'PBS_O_WORKDIR'
    args['in'].append('cd $%s' % work_dir)
    args['in'].append('echo Working directory is $%s' % work_dir)


def get_relocation(args: dict) -> None:
    """Collect all the relocation commands that move files and folders
    to/from scratch folder.
    This results in extending the `args` dictionary with the "in", "out" and
    "mv" keys that point to lists of command.

    Parameters
    ----------
    args : dict
        All arguments, including:
            scratch: str
                Path to scratch folder (to move files and compute)
            userscratch: str
                Path to user scratch folder (to move files and compute)
            localscratch: tuple
                Use localscratch with the provided memory amount (in gb)
    """
    if args['move']:
        args['in'] = []
        args['out'] = []
        args['mv'] = []
        # Get scratch folder creation and deletion commands
        get_scratching_commands(args)
        # Get command that move the inputs in and outputs out
        get_relocating_commands(args)
    else:
        # Switch to working directory; default is home directory
        go_to_work(args)
