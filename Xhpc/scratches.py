# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os
from os.path import isfile
from Xhpc.io_utils import get_first_line


def edit_scratch(args: dict, scratch_fp: str, scratch: str) -> str:
    """
    Checks that the content of the existing scratches file are valid paths.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following key is of interest:
            config_scratch : bool
                Show current scratches folder and/or edit it
    scratch_fp : str
        Path to the file containing the scratch folder
    scratch : str
        "scratch" or "userscratch"

    Returns
    -------
    scratch_folder : str
        Scratch folder
    """
    # parse the first line of the config file
    scratch_folder = get_first_line(scratch_fp)
    if args['config_scratch']:
        print("Registered %s: %s" % (scratch, scratch_folder))
        scratch_folder = create_scratch(args, scratch_fp, scratch)
    return scratch_folder


def get_scratches(args: dict, config_dir: str) -> None:
    """
    Collect the scratch folder paths from the scratch config file.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following key is of interest:
            scratch: str
                Path to scratch folder (to move files and compute)
            userscratch: str
                Path to user scratch folder (to move files and compute)
            config_scratch : bool
                Show current scratches folder and/or edit it
    config_dir : str
       Path to the folder that may (or not) contains the scratch paths files
    """
    for scratch in ['scratch', 'userscratch']:
        if args[scratch]:
            scratch_fp = '%s/%s.txt' % (config_dir, scratch)
            if isfile(scratch_fp):
                args[scratch] = edit_scratch(args, scratch_fp, scratch)
            else:
                args[scratch] = create_scratch(args, scratch_fp, scratch)


def create_scratch(args: dict, scratch_fp: str, scratch: str) -> str:
    """Collect the scratch folders interactively from the user
    and write it somewhere it can be reused.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following key is of interest:
            config_scratch : bool
                Show current scratches folder and/or edit it
    scratch_fp : str
        Path to the file containing the scratch folder
    scratch : str
        "scratch" or "userscratch"

    Returns
    -------
    scratch_folder : str
        Scratch folder
    """
    if args['config_scratch']:
        scratch_folder = get_scratch(scratch)
    elif scratch == 'userscratch' and 'USERWORK' in os.environ:
        scratch_folder = '${USERWORK}'
    else:
        scratch_folder = get_scratch(scratch)
    write_scratches(scratch_fp, scratch_folder)
    return scratch_folder


def get_scratch_area(scratch: str, default: str) -> str:
    """Collect the scratch/userscratch folder interactively from the user.

    Parameters
    ----------
    scratch : str
        "scratch" or "userscratch"
    default : str
        Default scratch folder on SAGA

    Returns
    -------
    scratch_folder : str
        Scratch folder
    """
    ret = input('(default [on SAGA]: "%s"): ' % default)
    if ret == 'y':
        scratch_folder = default
    else:
        if not isfile(ret):
            raise IOError('Enter a valid "%s" folder path...' % scratch)
        scratch_folder = ret
    return scratch_folder


def get_scratch(scratch: str) -> str:
    """Collect the scratch folders interactively from the user.

    Parameters
    ----------
    scratch : str
        "scratch" or "userscratch"

    Returns
    -------
    scratch_folder : str
        Scratch folder
    """
    print('Please enter %s location ("y" to confirm default)' % scratch)
    if scratch == 'userscratch':
        scratch_folder = get_scratch_area(scratch, '/cluster/work/users/$USER')
    else:
        scratch_folder = get_scratch_area(scratch, '/cluster/work/jobs')
    return scratch_folder


def write_scratches(scratch_fp: str, scratch_folder: str) -> None:
    """Write the scratch folder somewhere it can be reused.

    Parameters
    ----------
    scratch_fp : str
        Path to the config file
    scratch_folder : str
        Scratch folder
    """
    with open(scratch_fp, 'w') as o:
        o.write('%s\n' % scratch_folder)
    print('Written: %s' % scratch_fp)
    if scratch_folder == '${USERWORK}':
        print('Written automatically since your machine presets $USERNAME')
