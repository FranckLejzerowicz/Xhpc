# ----------------------------------------------------------------------------
# Copyright (c) 2020, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
import subprocess
from os.path import dirname
from Xhpc.env import *
import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestConda(unittest.TestCase):

    def setUp(self):
        self.env_name = 'testforXhpc'
        self.conda_bin = subprocess.getoutput('which conda')
        cmd = ['conda', 'create', '-n', self.env_name,
               'python', '--no-default-packages', '--yes']
        stdout, stderr = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        for i in stdout.decode().split('\n'):
            if 'environment location' in i:
                self.env_path = i.strip().split()[-1]
                libs = glob.glob('%s/bin/*' % self.env_path)
                self.conda_exe = set([basename(x) for x in libs])
                break
        self.envs_path = dirname(self.env_path)
        self.conda_bin_path = self.envs_path.replace('envs', 'bin/conda')

        self.dummy_paths = ['%s/path1' % test_folder, '%s/path2' % test_folder]
        self.dummy_executable_paths = []
        self.dummy_executables = {'a', 'b', 'c'}
        for dummy_path in self.dummy_paths:
            os.makedirs(dummy_path)
            for dummy_executable in self.dummy_executables:
                dummy_executable_path = dummy_path + '/' + dummy_executable
                self.dummy_executable_paths.append(dummy_executable_path)
                with open(dummy_executable_path, "w"):
                    pass

    def test_get_conda_path(self):
        obs = get_conda_path(self.env_path)
        self.assertEqual(self.env_path, obs)

        os.environ = {'CONDA_ENVS_PATH': self.envs_path}
        obs = get_conda_path(self.env_name)
        self.assertEqual(self.env_path, obs)

        os.environ = {'CONDA_EXE': self.conda_bin_path}
        obs = get_conda_path(self.env_name)
        self.assertEqual(self.env_path, obs)

        os.environ = {}
        with self.assertRaises(OSError) as cm:
            get_conda_path(self.env_name)
        self.assertEqual(str(cm.exception), 'Make sure conda is installed')

    def test_get_conda_executables(self):
        obs = get_conda_executables('/this/path/surely/does/not/exist')
        self.assertEqual(set(), obs)

        obs = get_conda_executables(self.env_path)
        self.assertEqual(self.conda_exe, obs)

        os.environ = {'CONDA_ENVS_PATH': self.envs_path}
        obs = get_conda_executables(self.env_name)
        self.assertEqual(self.conda_exe, obs)

        os.environ = {'CONDA_EXE': self.conda_bin_path}
        obs = get_conda_executables(self.env_name)
        self.assertEqual(self.conda_exe, obs)

        os.environ = {}
        with self.assertRaises(OSError) as cm:
            get_conda_executables(self.env_name)
        self.assertEqual(str(cm.exception), 'Make sure conda is installed')

    def test_get_path_executables(self):
        os.environ = {'PATH': ':'.join(list(self.dummy_paths))}
        obs = get_path_executables()
        self.assertEqual(self.dummy_executables, obs)

        os.environ = {'PATH': self.dummy_paths[0]}
        obs = get_path_executables()
        self.assertEqual(self.dummy_executables, obs)

        os.environ = {'OTHER_ENV': 'whatever'}
        obs = get_path_executables()
        self.assertEqual(set(), obs)

    def test_get_executables(self):
        os.environ = {'OTHER_ENV': 'whatever'}
        args = {'env': '/this/path/surely/does/not/exist'}
        get_executables(args)
        self.assertEqual(set(), args['executables'])

        args = {'env': self.env_path}
        get_executables(args)
        self.assertEqual(self.conda_exe, args['executables'])

        args = {'env': self.env_name}
        os.environ = {'CONDA_ENVS_PATH': self.envs_path}
        get_executables(args)
        self.assertEqual(self.conda_exe, args['executables'])

        args = {'env': self.env_name}
        os.environ = {'CONDA_EXE': self.conda_bin_path}
        get_executables(args)
        self.assertEqual(self.conda_exe, args['executables'])

        args = {'env': self.env_name}
        os.environ = {}
        with self.assertRaises(OSError) as cm:
            get_executables(args)
        self.assertEqual(str(cm.exception), 'Make sure conda is installed')

    def tearDown(self):
        cmd = [self.conda_bin, 'remove', '-n', self.env_name, '--all', '--yes']
        subprocess.call(cmd)
        for dummy_executable_path in self.dummy_executable_paths:
            os.remove(dummy_executable_path)
        for dummy_path in self.dummy_paths:
            os.rmdir(dummy_path)


if __name__ == '__main__':
    unittest.main()
