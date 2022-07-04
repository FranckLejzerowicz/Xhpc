# ----------------------------------------------------------------------------
# Copyright (c) 2020, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os
import unittest
from Xhpc.cmd import *
import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class Test(unittest.TestCase):

    def setUp(self):
        self.args = {
            'executables': {'exe'},
            'abspath': True,
            'exclude': (),
            'include': (),
            'move': False
        }
        self.path = './test_check_term.txt'
        with open(self.path, 'w') as o:
            pass
        self.abspath = os.path.abspath(self.path)
        self.paths = set()
        self.line_comment = '# line'
        self.line_simple = 'exe -i in -o out'
        self.line_simple_path = 'exe -i /in -o /out'
        self.line_exists = 'exe -i test_parse_line_in -o test_parse_line_out'
        with open('test_parse_line_in', 'w'):
            pass
        self.abspath_fp = os.path.abspath('test_parse_line_in')
        self.commands = []

        self.line_comment_fp = '%s/line_comment_fp.txt' % test_folder
        with open(self.line_comment_fp, 'w') as o:
            o.write('%s\n' % self.line_comment)
        self.line_simple_fp = '%s/line_simple_fp.txt' % test_folder
        with open(self.line_simple_fp, 'w') as o:
            o.write('%s\n' % self.line_simple)
        self.line_simple_path_fp = '%s/line_simple_path_fp.txt' % test_folder
        with open(self.line_simple_path_fp, 'w') as o:
            o.write('%s\n' % self.line_simple_path)
        self.line_exists_fp = '%s/line_exists_fp.txt' % test_folder
        with open(self.line_exists_fp, 'w') as o:
            o.write('%s\n' % self.line_exists)

    def test_check_term(self):
        obs = check_term(self.args, 'exe')
        self.assertEqual(obs, 'exe')
        obs = check_term(self.args, self.path)
        self.assertEqual(obs, self.abspath)
        obs = check_term(self.args, 'whatever')
        self.assertEqual(obs, 'whatever')

    def test_get_separated_terms(self):
        term, sep = 'no_sep', ','
        exp = ['no_sep']
        obs = get_separated_terms(term, sep)
        self.assertEqual(exp, obs)

        term, sep = 'sep1,sep2', ','
        exp = ['sep1', 'sep2']
        obs = get_separated_terms(term, sep)
        self.assertEqual(exp, obs)

        term, sep = 'sep1:sep2', ':'
        exp = ['sep1', 'sep2']
        obs = get_separated_terms(term, sep)
        self.assertEqual(exp, obs)

    def test_get_term(self):
        obs = get_term(self.args, 0, '')
        self.assertEqual(obs, '')
        obs = get_term(self.args, 0, 'exe')
        self.assertEqual(obs, 'exe')
        obs = get_term(self.args, 0, self.path)
        self.assertEqual(obs, self.abspath)
        obs = get_term(self.args, 0, 'whatever')
        self.assertEqual(obs, 'whatever')
        obs = get_term(self.args, 1, '')
        self.assertEqual(obs, '')
        obs = get_term(self.args, 1, 'exe')
        self.assertEqual(obs, 'exe')
        obs = get_term(self.args, 1, self.path)
        self.assertEqual(obs, self.abspath)
        obs = get_term(self.args, 1, '/whatever')
        self.assertEqual(obs, '/whatever')

    def test_split_quoted(self):
        obs = split_quoted('say "hello" here', '"')
        exp = [('say ', ''), ('hello', '"'), (' here', '')]
        self.assertEqual(exp, obs)
        obs = split_quoted("say 'hello' here", "'")
        exp = [('say ', ''), ('hello', "'"), (' here', '')]
        self.assertEqual(exp, obs)
        obs = split_quoted("say 'hello' here 'again'", "'")
        exp = [('say ', ''), ('hello', "'"), (' here ', ''),
               ('again', "'"), ('', '')]
        self.assertEqual(exp, obs)
        obs = split_quoted("'that' should never happen", "'")
        exp = [('', ''), ('that', "'"), (' should never happen', '')]
        self.assertEqual(exp, obs)

    def test_quote_split_line(self):
        obs = quote_split_line('say hello here')
        exp = [('say hello here', '')]
        self.assertEqual(exp, obs)
        obs = quote_split_line('say "hello" here')
        exp = [('say ', ''), ('hello', '"'), (' here', '')]
        self.assertEqual(exp, obs)
        obs = quote_split_line("say 'hello' here")
        exp = [('say ', ''), ('hello', "'"), (' here', '')]
        self.assertEqual(exp, obs)
        obs = quote_split_line("say 'hello' here 'again'")
        exp = [('say ', ''), ('hello', "'"), (' here ', ''),
               ('again', "'"), ('', '')]
        self.assertEqual(exp, obs)
        obs = quote_split_line("'that' should never happen")
        exp = [('', ''), ('that', "'"), (' should never happen', '')]
        self.assertEqual(exp, obs)

    def test_get_final_command_part(self):
        obs = get_final_command_part(['a', 'b'], '')
        exp = 'a b'
        self.assertEqual(exp, obs)
        obs = get_final_command_part(['a', 'b'], '"')
        exp = '"a b"'
        self.assertEqual(exp, obs)
        obs = get_final_command_part(['a', 'b'], "'")
        exp = "'a b'"
        self.assertEqual(exp, obs)

    def test_parse_line_comment(self):
        parse_line(self.line_comment, self.args, self.paths, self.commands)
        self.assertEqual(['# line'], self.commands)

    def test_parse_line_simple(self):
        parse_line(self.line_simple, self.args, self.paths, self.commands)
        self.assertEqual([self.line_simple], self.commands)
        self.assertEqual(self.paths, set())

    def test_parse_line_simple_path(self):
        line_simple_path = 'exe -i /in -o /out'
        parse_line(line_simple_path, self.args, self.paths, self.commands)
        self.assertEqual([line_simple_path], self.commands)
        self.assertEqual(self.paths, {'/in', '/out'})

    def test_parse_line_simple_path_move(self):
        self.args['move'] = True
        line_simple_path = 'exe -i /in -o /out'
        parse_line(line_simple_path, self.args, self.paths, self.commands)
        self.assertEqual([
            'exe -i ${SCRATCH_FOLDER}/in -o ${SCRATCH_FOLDER}/out'],
            self.commands)
        self.assertEqual(self.paths, {'/in', '/out'})

    def test_parse_line_exists(self):
        line_exists = 'exe -i test_parse_line_in -o test_parse_line_out'
        parse_line(line_exists, self.args, self.paths, self.commands)
        exp = ['exe -i %s -o test_parse_line_out' % self.abspath_fp]
        self.assertEqual(exp, self.commands)
        self.assertEqual(self.paths, {self.abspath_fp})

    def test_parse_line_complex(self):
        self.args['move'] = True
        line = 'name -i "/a,/b" -o /x,/y:/1:/2'
        parse_line(line, self.args, self.paths, self.commands)
        out = 'name -i "${SCRATCH_FOLDER}/a,${SCRATCH_FOLDER}/b" ' \
              '-o ${SCRATCH_FOLDER}'
        out += '/x,${SCRATCH_FOLDER}/y:${SCRATCH_FOLDER}/1:${SCRATCH_FOLDER}/2'
        exp = [out]
        self.assertEqual(exp, self.commands)
        self.assertEqual(self.paths, {'/a', '/b', '/x', '/y', '/1', '/2'})

    def test_parse_line_exists_move(self):
        self.args['move'] = True
        line_exists = 'exe -i test_parse_line_in -o test_parse_line_out'
        parse_line(line_exists, self.args, self.paths, self.commands)
        exp = ['exe -i ${SCRATCH_FOLDER}%s '
               '-o test_parse_line_out' % self.abspath_fp]
        self.assertEqual(exp, self.commands)
        self.assertEqual(self.paths, {self.abspath_fp})

    def test_get_commands_comment(self):
        self.args['input_fp'] = self.line_comment_fp
        get_commands(self.args)
        self.assertEqual(['# line'], self.args['commands'])

    def test_get_commands_simple(self):
        self.args['input_fp'] = self.line_simple_fp
        get_commands(self.args)
        self.assertEqual([self.line_simple], self.args['commands'])
        self.assertEqual(self.args['paths'], set())

    def test_get_commands_simple_path(self):
        self.args['input_fp'] = self.line_simple_path_fp
        get_commands(self.args)
        self.assertEqual([self.line_simple_path], self.args['commands'])
        self.assertEqual(self.args['paths'], {'/in', '/out'})

    def test_get_commands_exists(self):
        self.args['input_fp'] = self.line_exists_fp
        get_commands(self.args)
        exp = ['exe -i %s -o test_parse_line_out' % self.abspath_fp]
        self.assertEqual(exp, self.args['commands'])
        self.assertEqual(self.args['paths'], {self.abspath_fp})

    def test_get_commands_exists_move(self):
        self.args['input_fp'] = self.line_exists_fp
        self.args['move'] = True
        get_commands(self.args)
        exp = ['exe -i ${SCRATCH_FOLDER}%s '
               '-o test_parse_line_out' % self.abspath_fp]
        self.assertEqual(exp, self.args['commands'])
        self.assertEqual(self.args['paths'], {self.abspath_fp})

    def tearDown(self) -> None:
        os.remove(self.path)
        if isfile(self.abspath):
            os.remove(self.abspath)
        os.remove(self.line_comment_fp)
        os.remove(self.line_simple_fp)
        os.remove(self.line_simple_path_fp)
        os.remove(self.line_exists_fp)


if __name__ == '__main__':
    unittest.main()
