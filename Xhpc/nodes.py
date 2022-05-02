# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import pandas as pd


def get_nodelist(args: dict) -> str:
    """Get the directive for the listed nodes.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            nodes: tuple
                Node names
            torque: bool
                Adapt to Torque
            sinfo_pd : pd.DataFrame
                Per-node sinfo output for an extensive set of parameters

    Returns
    -------
    directive : str
        Current directive
    """
    if args['torque']:
        directive = '#PBS -l nodes=%s' % ','.join(args['nodes'])
        directive += '\n#PBS -l ppn=%s' % args['cpus']
    else:
        directive = '#SBATCH --nodelist=%s' % ','.join(args['nodes'])
        directive += '\n#SBATCH --ntasks-per-node=%s' % args['cpus']
    return directive


def get_nodes_ppn(args: dict) -> str:
    """Distribute the number of processors requested among the requested nodes.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            nnodes: int
                Number of nodes
            cpus: int
                Number of CPUs
            torque: bool
                Adapt to Torque
            sinfo_pd : pd.DataFrame
                Per-node sinfo output for an extensive set of parameters

    Returns
    -------
    directive : str
        Current directive
    """
    nnodes = int(args['nnodes'])  # number of nodes requested
    ncpus = int(args['cpus'])     # number of processors requested
    if args['torque']:
        directive = '#PBS -l nodes=%s:ppn=%s' % (nnodes, ncpus)
    else:
        if nnodes > 1 and ncpus > 1:
            directive = '#SBATCH --nodes=%s' % nnodes
            directive += '\n#SBATCH --ntasks-per-node=%s' % ncpus
        elif nnodes > 1:
            directive = '#SBATCH --nodes=%s' % nnodes
            directive += '\n#SBATCH --ntasks-per-node=1'
        elif ncpus > 1:
            directive = '#SBATCH --ntasks=%s' % ncpus
        else:
            directive = '#SBATCH --ntasks=1'
    return directive


def allocate_nodes(args: dict) -> str:
    """Look at the latest nodes/processors availability and make an
    allocation of such resources to satisfy the requested resources.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            nnodes: int
                Number of nodes
            sinfo_pd : pd.DataFrame
                Per-node sinfo output for an extensive set of parameters

    Returns
    -------
    directive : str
    """
    if args['sinfo_pd'].shape[0]:
        part_pd = subset_sinfo_pd(args)
        directive = get_allocated_nodes(args, part_pd)
    else:
        raise ValueError('There must have been an issue with collecting sinfo')
    return directive


def subset_sinfo_pd(args: dict) -> pd.DataFrame:
    """Subset the sinfo table to the requested partition and .

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            nnodes: int
                Number of nodes
            cpus: int
                Number of CPUs
            sinfo_pd : pd.DataFrame
                Per-node sinfo output for an extensive set of parameters

    Returns
    -------
    part_pd : pd.DataFrame
        sinfo_pd reduced to the queried partition and with expanded procs info
    """
    sinfo_pd = args['sinfo_pd']
    partition = args['partition']
    part_pd = sinfo_pd.loc[sinfo_pd.status.isin(['idle', 'mixed'])].copy()
    if not part_pd.shape[0]:
        raise ValueError(
            'No processors for allocation. Use `--sinfo` to update node info, '
            'or do not use `--allocate` to let Slurm scheduling your job...')
    part_pd = part_pd.loc[part_pd.partition.str.contains(partition)].copy()
    part_pd['mem_load'] = round(100*(1-(part_pd['free_mem']/part_pd['mem'])), 2)
    part_pd['idle'] = part_pd.cpus.apply(lambda x: x.split('/')[1])
    part_pd = part_pd.sort_values('idle', ascending=False)
    return part_pd


def get_allocated_nodes(args: dict, part_pd: pd.DataFrame) -> str:
    """Look at the latest nodes/processors availability and make an
    allocation of such resources to satisfy the requested resources.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            nnodes: int
                Number of nodes
            cpus: int
                Number of CPUs
            sinfo_pd : pd.DataFrame
                Per-node sinfo output for an extensive set of parameters
    part_pd : pd.DataFrame
        sinfo_pd reduced to the queried partition and with expanded procs info

    Returns
    -------
    directive : str
        Slurm directive to allocate specific nodes
    """
    nnodes, ncpus = int(args['nnodes']), int(args['cpus'])  # node/cpu resquest
    procs_wanted = nnodes * ncpus  # total number of processors wanted
    procs_alloc, nodes_alloc = 0, []

    # parse sinfo sorted from nodes with the most to the least avail cpus
    for row in part_pd.values:
        node = row[0]
        cpu_load = float(row[3])
        mem_load = float(row[-2])
        idle_procs = int(row[-1])
        # skip nodes that are too heavily loaded
        if cpu_load < 50 and mem_load < 50:
            procs_alloc += idle_procs
            nodes_alloc.append(node)
        # stop when enough nodes to accommodate all cpus are found
        if procs_alloc >= procs_wanted:
            break

    directive = '#SBATCH --nodelist=%s' % ','.join(nodes_alloc)
    directive += '\n#SBATCH --tasks=%s' % args['cpus']
    return directive
