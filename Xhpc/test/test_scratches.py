# ----------------------------------------------------------------------------
# Copyright (c) 2020, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import unittest
from unittest.mock import patch
from Xhpc.scratches import *
import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestScratches(unittest.TestCase):

    def setUp(self):
        self.args = {
            'scratch': False, 'userscratch': False, 'config_scratch': False}
        self.path = '%s/out.txt' % test_folder
        self.dir = '%s/config' % test_folder
        os.makedirs(self.dir)
        self.scratch_txt = '%s/scratch.txt' % self.dir
        self.userscratch_txt = '%s/userscratch.txt' % self.dir
        self.content = '/any/thing'
        self.userwork = '${USERWORK}'
        self.scratch_fp = './scratch.txt'
        with open(self.scratch_fp, 'w') as o:
            o.write('/some/scratch/folder\n')
        self.scratch_default = '/cluster/work/jobs'
        self.userscratch_default = '/cluster/work/users/$USER'

    def test_edit_scratch(self):
        obs = edit_scratch(self.args, self.scratch_fp, 'scratch')
        self.assertEqual('/some/scratch/folder', obs)

    @patch('builtins.input', return_value='y')
    def test_edit_scratch_y(self, input):
        self.args['config_scratch'] = True
        obs = edit_scratch(self.args, self.scratch_fp, 'scratch')
        self.assertEqual(self.scratch_default, obs)
        obs = edit_scratch(self.args, self.scratch_fp, 'userscratch')
        self.assertEqual(self.userscratch_default, obs)

    @patch('builtins.input', return_value='y')
    def test_get_scratches(self, input):
        self.assertIsNone(get_scratches(self.args, self.dir))

        self.args['userscratch'] = True
        get_scratches(self.args, self.dir)
        self.assertEqual(self.userscratch_default, self.args['userscratch'])
        self.assertTrue(isfile(self.userscratch_txt))

        self.args['scratch'] = True
        get_scratches(self.args, self.dir)
        self.assertEqual(self.scratch_default, self.args['scratch'])
        self.assertTrue(isfile(self.scratch_txt))

    def test_create_scratch(self):
        os.environ = {'USERWORK': 1}
        obs = create_scratch(self.args, self.scratch_fp, 'userscratch')
        self.assertEqual('${USERWORK}', obs)
        self.args['config_scratch'] = True
        self.assertEqual('${USERWORK}', obs)

    @patch('builtins.input', return_value='y')
    def test_create_scratch_input(self, input):
        os.environ = {'USERWORK': 1}
        obs = create_scratch(self.args, self.scratch_fp, 'scratch')
        self.assertEqual('/cluster/work/jobs', obs)
        os.environ = {}
        obs = create_scratch(self.args, self.scratch_fp, 'userscratch')
        self.assertEqual('/cluster/work/users/$USER', obs)
        obs = create_scratch(self.args, self.scratch_fp, 'scratch')
        self.assertEqual('/cluster/work/jobs', obs)

    @patch('builtins.input', return_value='y')
    def test_get_scratch_area_y(self, input):
        obs = get_scratch_area("scratch", 'a')
        self.assertEqual('a', obs)
        obs = get_scratch_area("userscratch", 'b')
        self.assertEqual('b', obs)

    @patch('builtins.input', return_value='n')
    def test_get_scratch_area_n_is_not(self, input):
        with self.assertRaises(IOError) as cm:
            get_scratch_area("scratch", '/not/a/valid/path')
        self.assertEqual(str(cm.exception),
                         'Enter a valid "scratch" folder path...')

        with self.assertRaises(IOError) as cm:
            get_scratch_area("userscratch", '/not/a/valid/path')
        self.assertEqual(str(cm.exception),
                         'Enter a valid "userscratch" folder path...')

    @patch('builtins.input', return_value='./scratch.txt')
    def test_get_scratch_area_n_is(self, input):
        obs = get_scratch_area("scratch", self.scratch_fp)
        self.assertEqual(self.scratch_fp, obs)

    @patch('builtins.input', return_value='y')
    def test_get_scratch_y(self, input):
        obs = get_scratch("scratch")
        self.assertEqual('/cluster/work/jobs', obs)
        obs = get_scratch("userscratch")
        self.assertEqual('/cluster/work/users/$USER', obs)

    @patch('builtins.input', return_value='n')
    def test_get_scratch_n_is_not(self, input):
        with self.assertRaises(IOError) as cm:
            get_scratch("scratch")
        self.assertEqual(str(cm.exception),
                         'Enter a valid "scratch" folder path...')

        with self.assertRaises(IOError) as cm:
            get_scratch("userscratch")
        self.assertEqual(str(cm.exception),
                         'Enter a valid "userscratch" folder path...')

    @patch('builtins.input', return_value='./scratch.txt')
    def test_get_scratch_n_is(self, input):
        obs = get_scratch("scratch")
        self.assertEqual(self.scratch_fp, obs)
        obs = get_scratch("localscratch")
        self.assertEqual(self.scratch_fp, obs)

    @patch('builtins.print')
    def test_write_scratches_print(self, prints):
        write_scratches(self.path, self.userwork)
        lines = []
        with open(self.path) as f:
            for line in f:
                lines.append(line.strip())
        self.assertEqual([self.userwork], lines)
        print_out = prints.call_args_list[0][0][0]
        self.assertEqual(print_out, 'Written: %s' % self.path)
        os.remove(self.path)

    def test_write_scratches(self):
        write_scratches(self.path, self.content)
        lines = []
        with open(self.path) as f:
            for line in f:
                lines.append(line.strip())
        self.assertEqual([self.content], lines)
        os.remove(self.path)

    def tearDown(self) -> None:
        os.remove(self.scratch_fp)
        if isfile(self.scratch_txt):
            os.remove(self.scratch_txt)
        if isfile(self.userscratch_txt):
            os.remove(self.userscratch_txt)
        if os.path.isdir(self.dir):
            os.rmdir(self.dir)


if __name__ == '__main__':
    unittest.main()
