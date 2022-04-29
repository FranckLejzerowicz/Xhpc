# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import itertools
import os
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
        args['scratching'].append('\n# Define and create a scratch directory')
        args['scratching'].append('SCRATCH_DIR="%s"' % scratch_path)
        args['scratching'].append('mkdir -p ${SCRATCH_DIR}')
        args['scratching'].append('cd ${SCRATCH_DIR}')
        args['scratching'].append('echo Working directory is ${SCRATCH_DIR}')
    else:
        args['scratching'].append('\n# Define scratch directory as working dir')
        if args['torque']:
            args['scratching'].append('SCRATCH_DIR=$PBS_O_WORKDIR')
        else:
            args['scratching'].append('SCRATCH_DIR=$SLURM_SUBMIT_DIR')


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


def get_min_files(files: set, min_folders: set) -> set:
    """Reduce the set of files to those not already within the folders.

    Parameters
    ----------
    files : set
        Sets of existing files that will be moved in scratch
    min_folders : set
        Keep the most basal of the existing folders passed in command

    Returns
    -------
    min_files : set
        Files not present with the minimum set of folders
    """
    min_files = set()
    for f1 in files:
        for f2 in min_folders:
            if f2 in dirname(f1):
                break
        else:
            min_files.add(f1)
    return min_files


def get_min_folders(folders: set, included: set) -> set:
    """Reduce the set of folder to that which is most basal so that

    Parameters
    ----------
    folders : set
        Sets of existing folder that will be moved in scratch
    included : set
        Paths to the folders that the user passed to option `--p-include`

    Returns
    -------
    min_folders : set
        Minimum set of existing folders to contain all folders passed in command
    """
    # Get the folders entailed within others (so that these need not be moved
    # specifically, they will be moved as being within these "others")
    min_folders, exclude = set(), set()
    for f1, f2 in itertools.combinations(sorted(folders), 2):
        if f1 != f2 and f1 in f2:
            exclude.add(f2)

    # Expand the collection of folders with those entailed within the
    # folders passed to option `--p-include` (same reason)
    for f1 in folders:
        for f2 in included:
            if f2 in f1:
                exclude.add(f1)

    # Actual reduction to remove all folders contained within a broader path
    min_folders = folders.difference(exclude)
    return min_folders


def get_include_commands(args: dict) -> set:
    """Get the "--include" option for the rsync command to exclude files
    and/or folder provided b the user.

    Parameters
    ----------
    args : dict
        All arguments, including:
            include : tuple
                Folder to not move to and from scratch using rsync (must exist)

    Returns
    -------
    included : set
        Paths to the folders that the user passed to option `--p-include`
    """
    included = set()
    if args['include']:
        m_to = []
        m_from = []
        for folder_ in args['include']:
            if not isdir(folder_):
                continue
            folder = os.path.abspath(folder_)
            included.add(folder)
            scratch = '${SCRATCH_DIR}%s' % folder
            args['mkdir'].update(['mkdir -p %s' % dirname(scratch),
                                  'mkdir -p %s' % folder])
            m_to.append('rsync -aqru %s/ %s' % (folder, scratch))
            m_from.append('rsync -aqru %s/ %s' % (scratch, folder))
        if m_to:
            args['move_to'].append('\n# Include command (move to scratch)')
            args['move_to'].extend(m_to)
        if m_from:
            args['move_from'].append('\n# Include command (move from scratch)')
            args['move_from'].extend(m_from)
    return included


def get_exclude(args: dict) -> str:
    """Get the "--exclude" option for the rsync command to exclude files
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


def move_to(args: dict, path: str, is_folder: bool, exclude: str = '') -> list:
    """

    Parameters
    ----------
    args : dict
        All arguments.
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
    args['mkdir'].add('mkdir -p %s' % dirname(destination))
    move = ['rsync -aqru %s %s%s' % (source, destination, exclude)]
    return move


def get_min_paths(in_out: dict, included: set) -> dict:
    """Get the minimum set of folders and files to move to scratch.

    Parameters
    ----------
    in_out : dict
        Sets of existing files and folder that will be moved in and
        of non-existing paths that will be move back.
    included : set
        Paths to the folders that the user passed to option `--p-include`

    Returns
    -------
    min_paths : dict
        Minimum set of existing folders to contain all folders passed in
        command (key "folders") and files not present with the minimum set of
        folders (key "files")
    """
    min_folders = get_min_folders(in_out['folders'], included)
    min_files = get_min_files(in_out['files'], min_folders)
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
    for min_folder in min_paths['folders']:
        args['move_to'].extend(move_to(args, min_folder, True, exclude))
    for min_file in min_paths['files']:
        args['move_to'].extend(move_to(args, min_file, False))


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
    for folder in min_paths['folders']:
        source = '${SCRATCH_DIR}%s' % folder
        args['mkdir'].add('mkdir -p %s' % folder)
        args['move_from'].append('rsync -auqr %s/ %s; fi' % (source, folder))
    for path in in_out['out']:
        source = '${SCRATCH_DIR}%s' % path
        args['move_from'].extend([
            'if [ -d %s ]; then mkdir -p %s; rsync -auqr %s/ %s; fi' % (
                source, path, source, path),
            'if [ -f %s ]; then rsync -auqr %s %s; fi' % (
                source, source, path)])


def get_relocating_commands(args: dict) -> None:
    """Get command that move the inputs in and outputs out.

    Parameters
    ----------
    args : dict
        All arguments, including:
            paths : set
                Paths to file or folders to move when using scratch folder
    """
    # Folders to move to and from scratch
    included = get_include_commands(args)
    # Folders not to move to scratch
    exclude = get_exclude(args)

    # Get paths to existing folders and files, and non-existing ones (outputs?)
    in_out = get_in_out(args['paths'])
    min_paths = get_min_paths(in_out, included)

    # Move in to scratch
    get_in_commands(args, min_paths, exclude)
    # Move out from scratch
    get_out_commands(args, min_paths, in_out)


def get_clearing_commands(args: dict):
    """Collect all the commands that clear the scratch location.

    Parameters
    ----------
    args : dict
        All arguments.
            clear_scratch : bool
                Whether to cleat the scratch area at the end of the job or not
            torque: bool
                Adapt to Torque
    """
    if args['clear_scratch']:
        # Get commands to move away and delete scratch directory
        args['clear'].append('\n# Move away and clear the scratch area')
        if args['torque']:
            args['clear'].append('cd ${PBS_O_WORKDIR}')
            args['clear'].append('rm -rf ${SCRATCH_DIR}')
        else:
            args['clear'].append('cd ${SLURM_SUBMIT_DIR}')
            args['clear'].append('rm -rf ${SCRATCH_DIR}')


def go_to_work(args: dict) -> None:
    """Get the working folder to go to and echo.

    Parameters
    ----------
    args : dict
        All arguments, including:
            torque: bool
                Adapt to Torque
    """
    args['move_to'] = ['# Move to the working directory and say it']
    work_dir = 'SLURM_SUBMIT_DIR'
    if args['torque']:
        work_dir = 'PBS_O_WORKDIR'
    args['move_to'].append('cd $%s' % work_dir)
    args['move_to'].append('echo Working directory is $%s' % work_dir)


def get_relocation(args: dict) -> None:
    """Collect all the relocation commands that move files and folders
    to/from scratch folder.
    This results in extending the `args` dictionary with the "in", "out" and
    "mv" keys that point to lists of command.

    Parameters
    ----------
    args : dict
        All arguments, including:
            move : bool
                Move files/folders to chosen scratch location
    """
    args['scratching'] = []
    args['mkdir'] = set()
    args['move_to'] = []
    args['move_from'] = []
    args['clear'] = []
    if args['move']:
        # Get scratch folder creation and deletion commands
        get_scratching_commands(args)
        # Get command that move the inputs in and outputs out
        get_relocating_commands(args)
        # Get command that clear the scratch location
        get_clearing_commands(args)
    else:
        # Switch to working directory; default is home directory
        go_to_work(args)
