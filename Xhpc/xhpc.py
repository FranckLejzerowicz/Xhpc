# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import subprocess
from os.path import expanduser
import pkg_resources as pkg_res

from Xhpc.email import get_email_address
from Xhpc.scratches import get_scratches
from Xhpc.env import get_executables
from Xhpc.directives import get_directives
from Xhpc.preamble import get_preamble
from Xhpc.cmd import get_commands
from Xhpc.relocate import get_relocation
from Xhpc.io_utils import (
    init_args, get_job_fp, get_output_dir, get_sinfo_pd, get_tmpdir,
    write_out, check_content, show_config, sys_exit)


def xhpc(**args) -> None:
    """
    Main script to go from .sh script to the Slurm or Torque output script file.
    Writes a Slurm/Torque script with directives and possibly re-localization
    of file if working on a (local)scratch folder.

    Parameters
    ----------
    args : dict
        All arguments, including:
            input_fp : str
                Input script path (or double-quoted command)
            job: str
                Job name
            output_fp: str
                Output script path
            account : str
                Name of the account
            partition: str
                Partition name
            out_dir: str
                Output directory
            env: str
                Conda environment to run the job
            nnodes: int
                Number of nodes
            cpus: int
                Number of CPUs
            time: str
                Wall time limit
            tmp: str
                Alternative temp folder
            mem: tuple
                Requested memory
            mem_per_cpu: tuple
                Requested memory
            nodes: tuple
                Node names
            workdir: str
                Working directory
            scratch: str
                Path to scratch folder (to move files and compute)
            userscratch: str
                Path to user scratch folder (to move files and compute)
            localscratch: tuple
                Use localscratch with the provided memory amount (in gb)
            clear_scratch : bool
                Whether to cleat the scratch area at the end of the job or not
            include : tuple
                Folder to not move to and from scratch using rsync (must exist)
            exclude : tuple
                Relative path(s) within input folder(s) to not move in scratch
            stdout : bool
                Rename stdout (and stderr) with job name and ID
            email: bool
                Send email (always if fail)
            run: bool
                Run the job output script
            move : bool
                Move files/folders to chosen scratch location
            abspath : bool
                Change the existing paths in command line to absolute paths
            verif: bool
                Print script to stdout and query user for sanity
            stat: bool
                Whether to prepend `/usr/bin/time -v` to every script command
            gpu: bool
                Query a gpu (experimental)
            torque: bool
                Adapt to Torque
            config_email: bool
                Show the current email and potentially edit it
            config_scratch : bool
                Show current scratches folder and/or edit it
            sinfo: bool
                Print sinfo in stdout (and update `~/.sinfo`)
            allocate : bool
                Get current machine usage to allocate suitable nodes/memory
            show_config : bool
                Show current configurations (email and scratches)
            quiet : bool
                Do not print anything
    """
    init_args(args)
    config_dir = '%s/user/%s' % (pkg_res.resource_filename("Xhpc", ""),
                                 os.environ['USER'])
    config_fp = '%s/config.txt' % config_dir
    if args['show_config']:
        show_config(args, config_fp)
    else:
        sys_exit(args)
        get_email_address(args, config_fp)  # get email address
        get_scratches(args, config_dir)  # get paths to scratch folders
        get_sinfo_pd(args, '%s/.sinfo' % expanduser('~'))  # get node usage
        get_job_fp(args)  # get path of output job file
        get_output_dir(args)  # get absolute path of the output directory
        get_executables(args)  # get set of executables in current environment
        get_directives(args)  # prepare job directives
        get_commands(args)  # get job command lines
        get_preamble(args)  # set environment and working directory
        get_tmpdir(args)  # set temporary directory
        get_relocation(args)   # arrange file movement
        # write the psb file to provide to "qsub"
        check_content(args)  # print-based, visual checks
        write_out(args)
        if args['run']:
            if not args['quiet']:
                print('Launched command: /bin/sh %s' % args['job_fp'])
            subprocess.call(['qsub', args['job_fp']])
