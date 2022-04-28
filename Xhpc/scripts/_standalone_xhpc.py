# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the MIT License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import click

from Xhpc.xhpc import xhpc
from Xhpc import __version__


@click.command()
@click.option(
    "-i", "--i-script", nargs=1,
    help="Input script path (or double-quoted command)")
@click.option(
    "-j", "--i-job", required=True, type=str, help="Job name")
@click.option(
    "-o", "--o-script", default=None, type=str,
    help="Output script path (default to <input>_<YYYY-MM-DD-HH-MM-SS>.slm)")
@click.option(
    "-a", "--p-account", default=None, type=str, help="Name of the account")
@click.option(
    "-p", "--p-partition", default='normal', help="Partition name",
    type=click.Choice(['normal', 'bigmem', 'accel', 'optimist']),)
@click.option(
    "-e", "--p-env", default=None, type=str,
    help="Conda environment to run the job")
@click.option(
    "-d", "--p-dir", help="Output directory", type=str,
    default='.', show_default=True)
@click.option(
    "-n", "--p-nnodes", default=1, type=int,
    help="Number of nodes", show_default=True)
@click.option(
    "-c", "--p-cpus", default=1, type=int,
    help="Number of CPUs", show_default=True)
@click.option(
    "-t", "--p-time", default=1, type=int,
    help="Wall time limit (in hours)", show_default=True)
@click.option(
    "-T", "--p-tmp", default=None, type=str,
    help="Alternative temp folder to the one defined in $TMPDIR (if not "
         "defined: will be set to $USERWORK, or to $SCRATCH if any scratch "
         "option is activated)")
@click.option(
    "-M", "--p-mem", nargs=2, show_default=False, default=('500', 'MB'),
    help="Requested memory as two space-separated entries: an integer and "
         "either 'MB' or 'GB'. (Default: '500 MB')")
@click.option(
    "-m", "--p-mem-per-cpu", nargs=2, show_default=False, default=None,
    help="Requested memory per cpu as two space-separated entries: an integer "
         "and either 'MB' or 'GB'. (Default: '500 MB')")
@click.option(
    "-N", "--p-nodes", default=None, multiple=True,
    help="Node names, e.g. `-N c1-4 -N c6-10 -N c7-1` (overrides option `-n`)")
@click.option(
    "-w", "--p-workdir", default=None, show_default=True,
    help="Working directory to use instead of $SLURM_SUBMIT_DIR")
@click.option(
    "-l", "--p-localscratch", type=int, show_default=False, default=None,
    help="Use localscratch with the provided memory amount (in GB)")
@click.option(
    "-x", "--p-exclude", multiple=True, default=None, show_default=True,
    help="Relative path(s) within input folder(s) to not move in scratch")
@click.option(
    "--scratch/--no-scratch", default=False, show_default=True,
    help="Use the scratch folder to move files and compute")
@click.option(
    "--userscratch/--no-userscratch", default=False, show_default=True,
    help="Use the userscratch folder to move files and compute")
@click.option(
    "--email/--no-email", default=False, show_default=True,
    help="Send email at job completion (always if fail)")
@click.option(
    "--run/--no-run", default=False, show_default=True,
    help="Run the job before exiting (subprocess)")
@click.option(
    "--move/--no-move", default=False, show_default=True,
    help="Move files/folders to chosen scratch location")
@click.option(
    "--verif/--no-verif", default=True, show_default=True,
    help="Print script to stdout and ask for 'y/n' user input to sanity check")
@click.option(
    "--gpu/--no-gpu", default=False, show_default=True,
    help="Query a gpu (experimental)")
@click.option(
    "--torque/--no-torque", default=False, show_default=True,
    help="Adapt to Torque")
@click.option(
    "--config-email/--no-config-email", default=False, show_default=True,
    help="Show current email and/or edit it")
@click.option(
    "--config-scratch/--no-config-scratch", default=False,
    show_default=True, help="Show current scratch folders and/or edit them")
@click.option(
    "--sinfo/--no-sinfo", default=False, show_default=True,
    help="Print sinfo in stdout (and update `~/.sinfo`)")
@click.option(
    "--allocate/--no-allocate", default=False, show_default=True,
    help="Get current machine usage (sinfo) to allocate suitable nodes/memory")
@click.version_option(__version__, prog_name="Xhpc")


def standalone_xhpc(
        i_script,
        i_job,
        o_script,
        p_account,
        p_partition,
        p_env,
        p_dir,
        p_nnodes,
        p_cpus,
        p_time,
        p_tmp,
        p_mem,
        p_mem_per_cpu,
        p_nodes,
        p_workdir,
        p_localscratch,
        p_exclude,
        scratch,
        userscratch,
        email,
        run,
        move,
        verif,
        gpu,
        torque,
        config_email,
        config_scratch,
        sinfo,
        allocate
):

    xhpc(
        input_fp=i_script,
        job=i_job,
        output_fp=o_script,
        account=p_account,
        partition=p_partition,
        env=p_env,
        out_dir=p_dir,
        nnodes=p_nnodes,
        cpus=p_cpus,
        time=p_time,
        tmp=p_tmp,
        mem=p_mem,
        mem_per_cpu=p_mem_per_cpu,
        nodes=p_nodes,
        localscratch=p_localscratch,
        exclude=p_exclude,
        scratch=scratch,
        userscratch=userscratch,
        workdir=p_workdir,
        email=email,
        run=run,
        move=move,
        verif=verif,
        gpu=gpu,
        torque=torque,
        config_email=config_email,
        config_scratch=config_scratch,
        sinfo=sinfo,
        allocate=allocate
    )


if __name__ == "__main__":
    standalone_xhpc()
