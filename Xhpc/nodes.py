# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------


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
    # a user might want several nodes but if there are enough processors
    # available on a lower number of nodes, then the number of nodes will be
    # reduced
    nodes = args['nodes']
    sinfo_pd = args['sinfo_pd']
    if sinfo_pd.shape[0]:
        nnodes = len(nodes)
        partition = args['partition']
        print(sinfo_pd)
        print(sinfo_pdd)
        nodelist = ','.join(nodes)
    else:
        nodelist = ','.join(nodes)
    if args['torque']:
        directive = '#PBS -l nodes=%s' % nodelist
    else:
        directive = '#SBATCH --nodelist=%s' % nodelist
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
            directive += '#SBATCH --ntasks-per-node=%s' % ncpus
        elif nnodes > 1:
            directive = '#SBATCH --nodes=%s' % nnodes
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
    sinfo_pd = args['sinfo_pd']
    directive = '#SBATCH --nodelist=%s' % alloc_nodes(args['nnodes'])
    return directive
