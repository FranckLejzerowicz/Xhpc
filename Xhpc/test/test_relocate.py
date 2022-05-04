# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os
import unittest
from Xhpc.relocate import *
import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestGetNodesPpn(unittest.TestCase):

    def setUp(self) -> None:
        self.args = {
            'job_id': 'X',
            'include': None,
            'exclude': None,
            'localscratch': None,
            'userscratch': False,
            'scratch': False,
            'torque': False,
            'move_to': [],
            'move_from': [],
            'clear': []
        }
        self.not_existing = ['%s/n1' % test_folder, '%s/n2.txt' % test_folder]
        self.folders = ['%s/d1' % test_folder, '%s/d2' % test_folder]
        for folder in self.folders:
            os.makedirs(folder)
        self.files = ['%s/f1.txt' % test_folder, '%s/f2.txt' % test_folder]
        for file in self.files:
            with open(file, 'w'):
                pass
        self.set = set(self.not_existing)
        self.set.update(set(self.folders))
        self.set.update(set(self.files))

    def test_get_scratch_path(self):
        self.assertIsNone(get_scratch_path(self.args))

        self.args['userscratch'] = '/some/path'
        obs = get_scratch_path(self.args)
        self.assertEqual('/some/path', obs)

        self.args['scratch'] = '/some/path'
        obs = get_scratch_path(self.args)
        self.assertEqual('/some/path', obs)

        self.args['localscratch'] = '/localscratch:10GB'
        obs = get_scratch_path(self.args)
        self.assertEqual('/localscratch:10GB', obs)

    def test_get_scratching_commands_noscratch_slurm(self):
        get_scratching_commands(self.args)
        exp = ['\n# Define scratch directory as working dir',
               'SCRATCH_DIR=$SLURM_SUBMIT_DIR']
        self.assertEqual(exp, self.args['move_to'])

    def test_get_scratching_commands_noscratch_torque(self):
        self.args['torque'] = True
        get_scratching_commands(self.args)
        exp = ['\n# Define scratch directory as working dir',
               'SCRATCH_DIR=$PBS_O_WORKDIR']
        self.assertEqual(exp, self.args['move_to'])

    def test_get_scratching_commands_local(self):
        self.args['localscratch'] = '/localscratch:10GB'
        get_scratching_commands(self.args)
        exp = ['\n# Define and create a scratch directory',
               'SCRATCH_DIR="/localscratch:10GB/${X}"',
               'mkdir -p ${SCRATCH_DIR}',
               'cd ${SCRATCH_DIR}',
               'echo Working directory is ${SCRATCH_DIR}']
        self.assertEqual(exp, self.args['move_to'])

    def test_get_scratching_commands_user(self):
        self.args['userscratch'] = '/some/path'
        get_scratching_commands(self.args)
        exp = ['\n# Define and create a scratch directory',
               'SCRATCH_DIR="/some/path/${X}"',
               'mkdir -p ${SCRATCH_DIR}',
               'cd ${SCRATCH_DIR}',
               'echo Working directory is ${SCRATCH_DIR}']
        self.assertEqual(exp, self.args['move_to'])

    def test_get_scratching_commands(self):
        self.args['scratch'] = '/some/path'
        get_scratching_commands(self.args)
        exp = ['\n# Define and create a scratch directory',
               'SCRATCH_DIR="/some/path/${X}"',
               'mkdir -p ${SCRATCH_DIR}',
               'cd ${SCRATCH_DIR}',
               'echo Working directory is ${SCRATCH_DIR}']
        self.assertEqual(exp, self.args['move_to'])

    def test_get_in_out(self):
        obs = get_in_out(self.set)
        exp = {'files': set(self.files),
               'folders': set(self.folders),
               'out': set(self.not_existing)}
        get_in_out(self.set)
        self.assertEqual(exp, obs)

    def test_get_min_files(self):
        files = {
            '/a/b/c/d/e.txt',
            '/a/b/c/d.txt',
            '/a/b/c.txt',
            '/1/2.txt',
            '/1.txt'
        }
        min_folders = {'/a/b/c', '/1'}
        obs = get_min_files(files, min_folders)
        exp = {'/a/b/c.txt', '/1.txt'}
        self.assertEqual(exp, obs)

    def test_get_min_folders(self):
        folders = {
            '/a/b/c/d/e',
            '/a/b/c/d',
            '/a/b/c',
            '/1/2/3/4/5',
            '/1'
        }
        obs = get_min_folders(folders, set())
        exp = {'/a/b/c', '/1'}
        self.assertEqual(exp, obs)

    def test_get_include_commands(self):
        obs = get_include_commands(self.args)
        exp = set()
        self.assertEqual(exp, obs)

        self.args['include'] = ('./a', './1/2/3',)
        obs = get_include_commands(self.args)
        self.assertEqual(exp, obs)

        os.makedirs('./a')
        path = os.path.abspath('./a')
        self.args['include'] = ('./a', './1/2/3',)
        obs = get_include_commands(self.args)
        self.assertEqual({path}, obs)
        path_dir = dirname(path)
        exp = ['\n# Include command (move to scratch)',
               'mkdir -p ${SCRATCH_DIR}%s' % path_dir,
               'rsync -aqru %s/ ${SCRATCH_DIR}%s' % (path, path)]
        self.assertEqual(exp, self.args['move_to'])
        exp = ['\n# Include command (move from scratch)',
               'mkdir -p %s' % path,
               'rsync -aqru ${SCRATCH_DIR}%s/ %s' % (path, path)]
        self.assertEqual(exp, self.args['move_from'])
        os.rmdir('./a')

    # def test_move_in(self):
    #     move_in()
    #
    # def test_get_in_commands(self):
    #     get_in_commands()
    #
    # def test_get_out_commands(self):
    #     get_out_commands()
    #
    # def test_get_relocating_commands(self):
    #     get_relocating_commands()
    #
    # def test_get_relocation(self):
    #     get_relocation()

    def test_go_to_work(self):
        go_to_work(self.args)
        exp = ['# Move to the working directory and say it',
               'cd $SLURM_SUBMIT_DIR',
               'echo Working directory is $SLURM_SUBMIT_DIR']
        self.assertEqual(exp, self.args['in'])

        self.args['torque'] = True
        go_to_work(self.args)
        exp = ['# Move to the working directory and say it',
               'cd $PBS_O_WORKDIR',
               'echo Working directory is $PBS_O_WORKDIR']
        self.assertEqual(exp, self.args['in'])

    def tearDown(self) -> None:
        self.folders = ['%s/d1' % test_folder, '%s/d2' % test_folder]
        for folder in self.folders:
            os.rmdir(folder)
        self.files = ['%s/f1.txt' % test_folder, '%s/f2.txt' % test_folder]
        for file in self.files:
            os.remove(file)


if __name__ == '__main__':
    unittest.main()
