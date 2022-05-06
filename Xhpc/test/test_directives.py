# ----------------------------------------------------------------------------
# Copyright (c) 2020, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
import pandas as pd
from Xhpc.directives import *
import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestConda(unittest.TestCase):

    def setUp(self):
        self.args = {
            'job': 'job_name',
            'account': 'account_name',
            'partition': 'normal',
            'workdir': '/some/path/here',
            'time': '10',
            'gpu': False,
            'torque': False,
            'email': True,
            'nodes': None,
            'allocate': None,
            'nnodes': 1,
            'cpus': 1,
            'sinfo_pd': pd.DataFrame()
        }
        self.email_address = 'some@email.com'

    def test_set_account(self):
        exp = '#SBATCH --account=%s' % self.args['account']
        obs = set_account(self.args)
        self.assertEqual(exp, obs)

        self.args['torque'] = True
        exp = '#PBS -A %s' % self.args['account']
        obs = set_account(self.args)
        self.assertEqual(exp, obs)

        self.args['account'] = None
        exp = None
        obs = set_account(self.args)
        self.assertEqual(exp, obs)

    def test_set_partition(self):
        exp = '#SBATCH --partition=normal'
        obs = set_partition(self.args)
        self.assertEqual(exp, obs)

        self.args['partition'] = 'whatever'
        exp = '#SBATCH --partition=whatever'
        obs = set_partition(self.args)
        self.assertEqual(exp, obs)

        self.args['gpu'] = True
        exp = '#SBATCH --partition=accel'
        obs = set_partition(self.args)
        self.assertEqual(exp, obs)

        self.args['torque'] = True
        exp = '#PBS -q whatever'
        obs = set_partition(self.args)
        self.assertEqual(exp, obs)

    def test_set_environment(self):
        exp = '#SBATCH --export=ALL'
        obs = set_environment(self.args)
        self.assertEqual(exp, obs)

        self.args['torque'] = True
        exp = '#PBS -V'
        obs = set_environment(self.args)
        self.assertEqual(exp, obs)

    def test_set_nodes_slurm(self):
        exp = '#SBATCH --ntasks=1'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['nnodes'] = 2
        exp = '#SBATCH --nodes=2\n#SBATCH --ntasks-per-node=1'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['cpus'] = 2
        exp = '#SBATCH --nodes=2\n#SBATCH --ntasks-per-node=2'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['nnodes'] = 1
        exp = '#SBATCH --ntasks=2'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

    def test_set_nodes_allocate(self):
        self.args['allocate'] = True

    def test_set_nodes_names(self):
        self.args['nodes'] = ('a', 'b', 'c',)
        exp = '#SBATCH --nodelist=a,b,c\n#SBATCH --ntasks-per-node=1'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['torque'] = True
        exp = '#PBS -l nodes=a,b,c\n#PBS -l ppn=1'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

    def test_set_nodes_torque(self):
        self.args['torque'] = True
        exp = '#PBS -l nodes=1:ppn=1'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['nnodes'] = 2
        exp = '#PBS -l nodes=2:ppn=1'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['cpus'] = 2
        exp = '#PBS -l nodes=2:ppn=2'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

        self.args['nnodes'] = 1
        exp = '#PBS -l nodes=1:ppn=2'
        obs = set_nodes(self.args)
        self.assertEqual(exp, obs)

    # def test_set_email(self):
    #     obs = set_email(self.args)
    #     self.assertEqual(exp, obs)
    #
    # def test_set_time(self):
    #     obs = set_time(self.args)
    #     self.assertEqual(exp, obs)
    #
    # def test_set_job(self):
    #     obs = set_job(self.args)
    #     self.assertEqual(exp, obs)
    #
    # def test_set_memory(self):
    #     obs = set_memory(self.args)
    #     self.assertEqual(exp, obs)
    #
    # def test_set_stdout_stderr(self):
    #     obs = set_stdout_stderr(self.args)
    #     self.assertEqual(exp, obs)
    #
    # def test_get_directives(self):
    #     obs = get_directives(self.args)
    #     self.assertEqual(exp, obs)


if __name__ == '__main__':
    unittest.main()
