# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from unittest import mock
from unittest.mock import patch
from datetime import datetime as dt

from pandas.testing import assert_frame_equal
from Xhpc.io_utils import *

import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestSinfo(unittest.TestCase):

    def setUp(self) -> None:
        self.sinfo_dir = '%s/.dummy_sinfo' % test_folder
        os.makedirs(self.sinfo_dir)

        self.config_folder = '%s/configs' % test_folder
        os.makedirs(self.config_folder)
        self.path1 = '%s/file1.txt' % self.config_folder
        self.path2 = '%s/file2.txt' % self.config_folder
        with open(self.path1, 'w') as o1, open(self.path2, 'w') as o2:
            o1.write('line1\nline2\nline3\n')
            o2.write('\n')

        self.today_fp = '%s/%s.tsv' % (self.sinfo_dir, str(dt.now().date()))

        self.sinfo_fps = []
        for day in [1, 2, 3]:
            self.sinfo = '%s/2022-01-0%s.tsv' % (self.sinfo_dir, day)
            with open(self.sinfo, 'w'):
                pass
            self.sinfo_fps.append(self.sinfo)

        self.stdout_bytes = b'some\ttab\tand\nreturn\tseparated\toutput\n'
        self.stdout_lists = [['some', 'tab', 'and'],
                             ['return', 'separated', 'output']]
        self.stdout_10cols = b'1 2 3 4 5 6 7 8 9 10\na b c d e f g h i j\n'
        self.stdout_pd = pd.DataFrame(
            [['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
             ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']],
            columns=['node', 'partition', 'status', 'cpu_load', 'cpus',
                     'socket', 'cores', 'threads', 'mem', 'free_mem'])

    def test_get_first_line(self):
        first_line = get_first_line(self.path1)
        self.assertEqual(first_line, 'line1')
        first_line = get_first_line(self.path2)
        self.assertEqual(first_line, '')

    def test_remove_previous_fps(self):
        for sinfo_fp in self.sinfo_fps:
            self.assertTrue(isfile(sinfo_fp))
        remove_previous_fps(self.sinfo_dir)
        for sinfo_fp in self.sinfo_fps:
            self.assertFalse(isfile(sinfo_fp))

    @unittest.mock.patch('subprocess.Popen')
    def test_run_sinfo(self, mock_popen):
        process_mock = unittest.mock.Mock()
        attrs = {'communicate.return_value': ('output', '')}
        process_mock.configure_mock(**attrs)
        mock_popen.return_value = process_mock
        run_sinfo_subprocess()
        self.assertTrue(mock_popen.called)

    @unittest.mock.patch('subprocess.Popen')
    def test_run_no_sinfo(self, mock_popen):
        process_mock = unittest.mock.Mock()
        attrs = {'communicate.return_value': ('', 'error')}
        process_mock.configure_mock(**attrs)
        mock_popen.return_value = process_mock
        with self.assertRaises(OSError) as cm:
            run_sinfo_subprocess()
        self.assertEqual(
            str(cm.exception), 'Using Slurm? `sinfo` command not found')

    def test_decode_sinfo_stdout(self):
        obs = decode_sinfo_stdout(self.stdout_bytes)
        exp = self.stdout_lists
        self.assertEqual(exp, obs)

        obs = decode_sinfo_stdout(b'a b c\nd e f\n')
        exp = [['a', 'b', 'c'], ['d', 'e', 'f']]
        self.assertEqual(exp, obs)

        obs = decode_sinfo_stdout(b'a    b    c\nd    e    f')
        exp = [['a', 'b', 'c'], ['d', 'e', 'f']]
        self.assertEqual(exp, obs)

    @unittest.mock.patch('subprocess.Popen')
    def test_collect_sinfo_pd(self, mock_popen):
        process_mock = unittest.mock.Mock()
        attrs = {'communicate.return_value': (self.stdout_10cols, '')}
        process_mock.configure_mock(**attrs)
        mock_popen.return_value = process_mock
        obs = collect_sinfo_pd()
        exp = self.stdout_pd
        assert_frame_equal(exp, obs)

    @unittest.mock.patch('subprocess.Popen')
    def test_failed_collect_sinfo_pd(self, mock_popen):
        process_mock = unittest.mock.Mock()
        attrs = {'communicate.return_value': ('', 'error')}
        process_mock.configure_mock(**attrs)
        mock_popen.return_value = process_mock
        with self.assertRaises(OSError) as cm:
            collect_sinfo_pd()
        self.assertEqual(
            str(cm.exception), 'Using Slurm? `sinfo` command not found')

    def test_get_sinfo_pd_expection(self):
        exception = 'No node allocation yet avail for PBS/Torque'
        with self.assertRaises(IOError) as cm:
            args = {'sinfo': False, 'allocate': False, 'torque': True}
            get_sinfo_pd(args, '')
        self.assertEqual(str(cm.exception), exception)
        with self.assertRaises(IOError) as cm:
            args = {'sinfo': True, 'allocate': True, 'torque': True}
            get_sinfo_pd(args, '')
        self.assertEqual(str(cm.exception), exception)
        with self.assertRaises(IOError) as cm:
            args = {'sinfo': True, 'allocate': False, 'torque': True}
            get_sinfo_pd(args, '')
        self.assertEqual(str(cm.exception), exception)
        with self.assertRaises(IOError) as cm:
            args = {'sinfo': False, 'allocate': True, 'torque': True}
            get_sinfo_pd(args, '')
        self.assertEqual(str(cm.exception), exception)

    def test_get_sinfo_pd_empty(self):
        args = {'sinfo': False, 'allocate': False, 'torque': False}
        get_sinfo_pd(args, '')
        assert_frame_equal(pd.DataFrame(), args['sinfo_pd'])

    def test_get_sinfo_pd_read(self):
        self.stdout_pd.to_csv(self.today_fp, index=False, sep='\t')
        args = {'sinfo': False, 'allocate': True, 'torque': False}
        get_sinfo_pd(args, self.sinfo_dir)
        exp = self.stdout_pd
        assert_frame_equal(exp, args['sinfo_pd'])
        os.remove(self.today_fp)

    @unittest.mock.patch('subprocess.Popen')
    def test_get_sinfo_pd_write(self, mock_popen):
        process_mock = unittest.mock.Mock()
        attrs = {'communicate.return_value': (self.stdout_10cols, '')}
        process_mock.configure_mock(**attrs)
        mock_popen.return_value = process_mock
        args = {'sinfo': True, 'allocate': False, 'torque': False}
        get_sinfo_pd(args, self.sinfo_dir)
        assert_frame_equal(self.stdout_pd, args['sinfo_pd'])

    def tearDown(self) -> None:
        for sinfo_fp in self.sinfo_fps:
            if isfile(sinfo_fp):
                os.remove(sinfo_fp)
        if isfile(self.today_fp):
            os.remove(self.today_fp)
        os.rmdir(self.sinfo_dir)
        for path in [self.path1, self.path2]:
            if os.path.isfile(path):
                os.remove(path)
        for folder in [self.config_folder]:
            os.rmdir(folder)


class TestOutput(unittest.TestCase):

    def setUp(self) -> None:
        self.pwd = abspath('.')
        self.real_dir = '%s/existing_dir' % test_folder
        os.makedirs(self.real_dir)
        self.fake_dir = '%s/non_existing_dir' % test_folder
        self.time = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    def test_get_job_fp(self):
        # no output path given and default, slurm usage

        args = {'job': 'a_name', 'output_fp': None, 'torque': False}
        get_job_fp(args)
        exp = '%s/a_name_%s.slm' % (self.pwd, self.time)
        self.assertEqual(exp, args['job_fp'])

        # no output path given and torque usage
        args = {'job': 'a_name', 'output_fp': None, 'torque': True}
        get_job_fp(args)
        exp = '%s/a_name_%s.pbs' % (self.pwd, self.time)
        self.assertEqual(exp, args['job_fp'])

        # output path given and default, slurm usage
        slm_fp = '/a/path/to/should/exists.slm'
        args = {'job': 'a_name', 'output_fp': slm_fp, 'torque': False}
        get_job_fp(args)
        self.assertEqual(slm_fp, args['job_fp'])

        # output path given and torque usage
        pbs_fp = '/a/path/to/should/exists.pbs'
        args = {'job': 'a_name', 'output_fp': pbs_fp, 'torque': True}
        get_job_fp(args)
        self.assertEqual(pbs_fp, args['job_fp'])

    def test_get_output_dir(self):
        args = {'out_dir': self.real_dir}
        get_output_dir(args)
        self.assertEqual(self.real_dir, args['output_dir'])

        args = {'out_dir': self.fake_dir}
        get_output_dir(args)
        self.assertEqual(self.pwd, args['output_dir'])

        args = {'out_dir': 'some/relative/folder'}
        get_output_dir(args)
        self.assertEqual(self.pwd, args['output_dir'])

        args = {'out_dir': 'some_relative_folder'}
        exp = '%s/some_relative_folder' % self.pwd
        os.makedirs(exp)
        get_output_dir(args)
        self.assertEqual(exp, args['output_dir'])
        os.rmdir(exp)

    def tearDown(self) -> None:
        os.rmdir(self.real_dir)


class TestCheckContent(unittest.TestCase):

    def setUp(self) -> None:
        self.args = {'directives': ['A'], 'preamble': ['B'],
                     'commands': ['C'], 'verif': True}

    def test_get_lines(self):
        obs = get_lines(['2', '1'])
        exp = ['2', '1']
        self.assertEqual(exp, obs)

        obs = get_lines({'2', '1'})
        exp = ['1', '2']
        self.assertEqual(exp, obs)

        with self.assertRaises(TypeError) as cm:
            get_lines('a')
        self.assertEqual(str(cm.exception),
                         'Commands are collected either as list or set')

    def test_no_check_content(self):
        self.args = {'verif': False}
        self.assertIsNone(check_content(self.args))

    @patch('builtins.input', return_value='n')
    def test_check_content(self, input):
        with self.assertRaises(SystemExit) as cm:
            check_content(self.args)
        self.assertEqual(cm.exception.code, 1)


class TestWriteOut(unittest.TestCase):

    def setUp(self) -> None:
        self.out = '%s/dummy_output.txt' % test_folder
        self.content_base = [
            'A\n', '# ------ directives END ------\n', '\n',
            'B\n', '# ------ preamble END ------\n', '\n',
            'D\n', '# ------ commands END ------\n', '\n',
            'echo "Done!"\n']
        self.content_time = [
            'A\n', '# ------ directives END ------\n', '\n',
            'B\n', '# ------ preamble END ------\n', '\n',
            '/usr/bin/time -v cmd \\\n', '\t-i X \\\n', '\t-e Y\n',
            '# ------ commands END ------\n', '\n',
            'echo "Done!"\n']
        self.content_move = [
            'A\n', '# ------ directives END ------\n', '\n',
            'B\n', '# ------ preamble END ------\n', '\n',
            'C\n', '# ------ move_to END ------\n', '\n',
            'D\n', '# ------ commands END ------\n', '\n',
            'E\n', '# ------ move_from END ------\n', '\n',
            'F\n', '# ------ clear END ------\n', '\n',
            'echo "Done!"\n']
        self.args = {
            'job_fp': self.out,
            'directives': {'A'},
            'preamble': {'B'},
            'move_to': set(),
            'commands': ['D'],
            'move_from': set(),
            'clear': set(),
            'move': False,
            'stat': False,
            'quiet': False
        }

    def test_write_out(self):
        write_out(self.args)
        lines = []
        with open(self.out) as f:
            for line in f:
                lines.append(line)
        self.assertEqual(lines, self.content_base)

    def test_write_out_time(self):
        self.args['stat'] = True
        self.args['commands'] = ['cmd \\', '\t-i X \\', '\t-e Y']
        write_out(self.args)
        lines = []
        with open(self.out) as f:
            for line in f:
                lines.append(line)
        self.assertEqual(lines, self.content_time)

    def test_write_out_move(self):
        self.args['move'] = True
        self.args['move_to'] = {'C'}
        self.args['move_from'] = {'E'}
        self.args['clear'] = {'F'}
        write_out(self.args)
        lines = []
        with open(self.out) as f:
            for line in f:
                lines.append(line)
        self.assertEqual(lines, self.content_move)

    def tearDown(self) -> None:
        os.remove(self.out)


if __name__ == '__main__':
    unittest.main()
