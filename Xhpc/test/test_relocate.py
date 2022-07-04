# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import unittest
from Xhpc.relocate import *
import pkg_resources


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
            'scratching': [],
            'move_from': set(),
            'move_to': set(),
            'mkdir': set(),
            'clear': [],
            'move': True,
            'quiet': False
        }
        self.test_folder = pkg_resources.resource_filename("Xhpc", "test")
        self.folder_of_not_existing = self.test_folder
        self.not_existing = ['%s/n1' % self.test_folder,
                             '%s/n2.txt' % self.test_folder]
        self.folders = ['%s/d1' % self.test_folder,
                        '%s/d2' % self.test_folder]
        for folder in self.folders:
            os.makedirs(folder)
        self.files = ['%s/f1.txt' % self.test_folder,
                      '%s/f2.txt' % self.test_folder]
        for file in self.files:
            with open(file, 'w'):
                pass
        self.set = set(self.not_existing)
        self.set.update(set(self.folders))
        self.set.update(set(self.files))

    def test_get_scratch_path(self):

        self.args['userscratch'] = '/some/path'
        with self.assertRaises(IOError) as cm:
            get_scratch_path(self.args)
        self.assertEqual(
            str(cm.exception),
            'Must specify `--scratch`, `--userscratch` or '
            '`--localscratch to use `--move`')

        self.args['scratch'] = '/some/path'
        with self.assertRaises(IOError) as cm:
            get_scratch_path(self.args)
        self.assertEqual(
            str(cm.exception),
            'Must specify `--scratch`, `--userscratch` or '
            '`--localscratch to use `--move`')

        self.args['localscratch'] = '/localscratch:10GB'
        with self.assertRaises(IOError) as cm:
            get_scratch_path(self.args)
        self.assertEqual(
            str(cm.exception),
            'Must specify `--scratch`, `--userscratch` or '
            '`--localscratch to use `--move`')

    def test_get_scratch_path(self):
        self.assertIsNone(get_scratch_path(self.args))

        self.args['move'] = True
        self.args['userscratch'] = '/some/path'
        obs = get_scratch_path(self.args)
        self.assertEqual('/some/path', obs)

        self.args['scratch'] = '/some/path'
        obs = get_scratch_path(self.args)
        self.assertEqual('/some/path', obs)

        self.args['localscratch'] = '/localscratch:10GB'
        obs = get_scratch_path(self.args)
        self.assertEqual('/localscratch:10GB', obs)

    def test_get_scratching_commands_local(self):
        self.args['localscratch'] = '/localscratch:10GB'
        get_scratching_commands(self.args)
        exp = ['# Define and create a scratch directory',
               'SCRATCH_FOLDER="/localscratch:10GB/${X}"',
               'mkdir -p ${SCRATCH_FOLDER}',
               'cd ${SCRATCH_FOLDER}',
               'echo Working directory is ${SCRATCH_FOLDER}']
        self.assertEqual(exp, self.args['scratching'])

    def test_get_scratching_commands_user(self):
        self.args['userscratch'] = '/some/path'
        get_scratching_commands(self.args)
        exp = ['# Define and create a scratch directory',
               'SCRATCH_FOLDER="/some/path/${X}"',
               'mkdir -p ${SCRATCH_FOLDER}',
               'cd ${SCRATCH_FOLDER}',
               'echo Working directory is ${SCRATCH_FOLDER}']
        self.assertEqual(exp, self.args['scratching'])

    def test_get_scratching_commands(self):
        self.args['scratch'] = '/some/path'
        get_scratching_commands(self.args)
        exp = ['# Define and create a scratch directory',
               'SCRATCH_FOLDER="/some/path/${X}"',
               'mkdir -p ${SCRATCH_FOLDER}',
               'cd ${SCRATCH_FOLDER}',
               'echo Working directory is ${SCRATCH_FOLDER}']
        self.assertEqual(exp, self.args['scratching'])

    def test_get_in_out(self):
        obs = get_in_out(self.set)
        exp = {'files': set(self.files),
               'folders': set(self.folders) | {self.folder_of_not_existing},
               'out': set()}
        # get_in_out(self.set)
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
        exp = set()
        obs = get_include_commands(self.args)
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
        exp = {'mkdir -p ${SCRATCH_FOLDER}%s' % path_dir}
        self.assertEqual(exp, self.args['mkdir'])
        exp = {'rsync -aqru %s/ ${SCRATCH_FOLDER}%s' % (path, path)}
        self.assertEqual(exp, self.args['move_to'])
        exp = {'rsync -aqru ${SCRATCH_FOLDER}%s/ %s' % (path, path)}
        self.assertEqual(exp, self.args['move_from'])
        os.rmdir('./a')

    def test_go_to_work(self):
        go_to_work(self.args)
        exp = ['# Move to the working directory and say it',
               'cd $SLURM_SUBMIT_DIR',
               'echo Working directory is $SLURM_SUBMIT_DIR']
        self.assertEqual(exp, self.args['move_to'])

        self.args['torque'] = True
        go_to_work(self.args)
        exp = ['# Move to the working directory and say it',
               'cd $PBS_O_WORKDIR',
               'echo Working directory is $PBS_O_WORKDIR']
        self.assertEqual(exp, self.args['move_to'])

    def tearDown(self) -> None:
        self.folders = ['%s/d1' % self.test_folder,
                        '%s/d2' % self.test_folder]
        for folder in self.folders:
            os.rmdir(folder)
        self.files = ['%s/f1.txt' % self.test_folder,
                      '%s/f2.txt' % self.test_folder]
        for file in self.files:
            os.remove(file)


if __name__ == '__main__':
    unittest.main()
