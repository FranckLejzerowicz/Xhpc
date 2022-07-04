# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from Xhpc.nodes import *


class TestGetNodesPpn(unittest.TestCase):

    def setUp(self) -> None:
        self.args = {'torque': False, 'nnodes': 1, 'cpus': 1}

    def test_get_nodes_ppn(self):
        self.args['torque'] = True
        obs = get_nodes_ppn(self.args)
        exp = '#PBS -l nodes=1:ppn=1'
        self.assertEqual(exp, obs)

        self.args['torque'] = False
        obs = get_nodes_ppn(self.args)
        exp = '#SBATCH --ntasks=1'
        self.assertEqual(exp, obs)

        self.args['cpus'] = 2
        obs = get_nodes_ppn(self.args)
        exp = '#SBATCH --ntasks=2'
        self.assertEqual(exp, obs)

        self.args['nnodes'] = 2
        obs = get_nodes_ppn(self.args)
        exp = '#SBATCH --nodes=2\n#SBATCH --ntasks-per-node=2'
        self.assertEqual(exp, obs)

        self.args['cpus'] = 1
        obs = get_nodes_ppn(self.args)
        exp = '#SBATCH --nodes=2\n#SBATCH --ntasks-per-node=1'
        self.assertEqual(exp, obs)


class TestGetNodelist(unittest.TestCase):

    def test_get_nodelist(self):
        args = {'torque': False, 'nodes': ('a', 'b',), 'cpus': 1}

        obs = get_nodelist(args)
        exp = '#SBATCH --nodelist=a,b\n#SBATCH --ntasks-per-node=1'
        self.assertEqual(exp, obs)

        args['torque'] = True
        obs = get_nodelist(args)
        exp = '#PBS -l nodes=a,b\n#PBS -l ppn=1'
        self.assertEqual(exp, obs)


if __name__ == '__main__':
    unittest.main()
