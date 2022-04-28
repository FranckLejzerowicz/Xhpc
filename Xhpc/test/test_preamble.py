# ----------------------------------------------------------------------------
# Copyright (c) 2020, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from Xhpc.preamble import *


class TestAddEnv(unittest.TestCase):

    def setUp(self):
        self.env = 'conda_env'
        self.args = {'env': None}
        self.preamble_cmd = ['\n# general environment info / behaviour',
                             'uname -a', 'set -e', 'set -u']
        self.env_cmd = ['\n# active conda environment',
                        'echo "Conda environment is %s"' % self.env,
                        'source activate %s' % self.env]

    def test_add_env_none(self):
        add_env(self.args)
        exp = self.preamble_cmd
        self.assertEqual(self.args['preamble'], exp)

    def test_add_env_empty(self):
        self.assertIsNone(self.args.get('preamble'))

    def test_add_env_return(self):
        self.assertIsNone(add_env(self.args))

    def test_add_env(self):
        self.args['env'] = self.env
        add_env(self.args)
        exp = self.preamble_cmd + self.env_cmd
        self.assertEqual(self.args['preamble'], exp)


class TestAddWorkdir(unittest.TestCase):

    def setUp(self):
        self.args = {'workdir': None, 'torque': False, 'preamble': []}
        self.workdir = '/any/path'
        self.torque = "export PBS_O_WORKDIR='%s'" % self.workdir
        self.slurm = "export SLURM_SUBMIT_DIR='%s'" % self.workdir
        self.torque_cmd = ['\n# change working directory', self.torque]
        self.slurm_cmd = ['\n# change working directory', self.slurm]

    def test_add_workdir_empty(self):
        self.assertIsNone(add_workdir(self.args))

    def test_add_workdir_slurm(self):
        self.args['workdir'] = self.workdir
        add_workdir(self.args)
        self.assertEqual(self.args['preamble'], self.slurm_cmd)

    def test_add_workdir_torque(self):
        self.args['workdir'] = self.workdir
        self.args['torque'] = True
        add_workdir(self.args)
        self.assertEqual(self.args['preamble'], self.torque_cmd)


class TestAddTmpdir(unittest.TestCase):

    def setUp(self):
        os.environ = {}
        self.args = {
            'tmp': False, 'move': False, 'torque': False,
            'job': '{A}', 'preamble': []}
        self.tmpdir_passed = '/any/path'
        self.tmpdir_move = '${SCRATCH_DIR}/tmpdir'
        self.tmpdir_os = '${TMPDIR}'

    def test_add_tmpdir_none(self):
        self.assertFalse(add_tmpdir(self.args))

    def test_add_tmpdir_tmp_slurm(self):
        self.args['tmp'] = self.tmpdir_passed
        self.tmpdir_passed += '/' + self.args['job'] + '_${SLURM_JOB_ID}'
        exp = [
            '\n# create and export the temporary directory',
            'mkdir -p %s' % self.tmpdir_passed,
            'export TMPDIR="%s"' % self.tmpdir_passed]
        add_tmpdir(self.args)
        self.assertEqual(exp, self.args['preamble'])

    def test_add_tmpdir_tmp_torque(self):
        self.args['tmp'] = self.tmpdir_passed
        self.args['torque'] = True
        self.tmpdir_passed += '/' + self.args['job'] + '_${PBS_JOBID}'
        exp = [
            '\n# create and export the temporary directory',
            'mkdir -p %s' % self.tmpdir_passed,
            'export TMPDIR="%s"' % self.tmpdir_passed]
        add_tmpdir(self.args)
        self.assertEqual(exp, self.args['preamble'])

    def test_add_tmpdir_move_slurm(self):
        self.args['tmp'] = self.tmpdir_move
        self.tmpdir_move += '/' + self.args['job'] + '_${SLURM_JOB_ID}'
        exp = [
            '\n# create and export the temporary directory',
            'mkdir -p %s' % self.tmpdir_move,
            'export TMPDIR="%s"' % self.tmpdir_move]
        add_tmpdir(self.args)
        self.assertEqual(exp, self.args['preamble'])

    def test_add_tmpdir_move_torque(self):
        self.args['tmp'] = self.tmpdir_move
        self.args['torque'] = True
        self.tmpdir_move += '/' + self.args['job'] + '_${PBS_JOBID}'
        exp = [
            '\n# create and export the temporary directory',
            'mkdir -p %s' % self.tmpdir_move,
            'export TMPDIR="%s"' % self.tmpdir_move]
        add_tmpdir(self.args)
        self.assertEqual(exp, self.args['preamble'])

    def test_add_tmpdir_os_slurm(self):
        os.environ['TMPDIR'] = 1
        self.tmpdir_os += '/' + self.args['job'] + '_${SLURM_JOB_ID}'
        exp = [
            '\n# create and export the temporary directory',
            'mkdir -p %s' % self.tmpdir_os,
            'export TMPDIR="%s"' % self.tmpdir_os]
        add_tmpdir(self.args)
        self.assertEqual(exp, self.args['preamble'])

    def test_add_tmpdir_os_torque(self):
        os.environ['TMPDIR'] = 1
        self.args['torque'] = True
        self.tmpdir_os += '/' + self.args['job'] + '_${PBS_JOBID}'
        exp = [
            '\n# create and export the temporary directory',
            'mkdir -p %s' % self.tmpdir_os,
            'export TMPDIR="%s"' % self.tmpdir_os]
        add_tmpdir(self.args)
        self.assertEqual(exp, self.args['preamble'])


class TestAddProcsNodes(unittest.TestCase):

    def setUp(self):
        self.args = {'torque': False, 'preamble': []}
        self.slurm = '$SLURM_JOB_NODELIST'
        self.torque = '$PBS_NODEFILE'

    def test_add_procs_nodes_slurm(self):
        add_procs_nodes(self.args)
        exp = ['NPROCS=`wc -l < %s`' % self.slurm,
               'NNODES=`uniq %s | wc -l`' % self.slurm]
        self.assertEqual(exp, self.args['preamble'])

    def test_add_procs_nodes_torque(self):
        self.args['torque'] = True
        add_procs_nodes(self.args)
        exp = ['NPROCS=`wc -l < %s`' % self.torque,
               'NNODES=`uniq %s | wc -l`' % self.torque]
        self.assertEqual(exp, self.args['preamble'])


class TestAddEchoes(unittest.TestCase):

    def setUp(self):
        self.args = {'torque': False, 'preamble': [], 'std_path': 'X'}
        self.part1 = ['\n# echo some info about the job',
                      'echo Running on host `hostname`',
                      'echo Time is `date`', 'echo Directory is `pwd`']
        self.part2 = ["echo Temporary directory is $TMPDIR"]
        self.slurm = ['NPROCS=`wc -l < $SLURM_JOB_NODELIST`',
                      'NNODES=`uniq $SLURM_JOB_NODELIST | wc -l`']
        self.torque = ['NPROCS=`wc -l < $PBS_NODEFILE`',
                       'NNODES=`uniq $PBS_NODEFILE | wc -l`']
        self.part3 = ['echo Use ${NPROCS} procs on ${NNODES} nodes',
                      'echo Job stdout is X.o', 'echo Job stderr is X.e']

    def test_add_echoes_notmp_slurm(self):
        exp = self.part1 + self.part2 + self.slurm + self.part3
        add_echoes(self.args, False)
        self.assertEqual(self.args['preamble'], exp)

    def test_add_echoes_notmp_torque(self):
        self.args['torque'] = True
        exp = self.part1 + self.part2 + self.torque + self.part3
        add_echoes(self.args, False)
        self.assertEqual(self.args['preamble'], exp)

    def test_add_echoes_tmp_slurm(self):
        exp = self.part1 + self.slurm + self.part3
        add_echoes(self.args, True)
        self.assertEqual(self.args['preamble'], exp)

    def test_add_echoes_tmp_torque(self):
        self.args['torque'] = True
        exp = self.part1 + self.torque + self.part3
        add_echoes(self.args, True)
        self.assertEqual(self.args['preamble'], exp)


if __name__ == '__main__':
    unittest.main()
