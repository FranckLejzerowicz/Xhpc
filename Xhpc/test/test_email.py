# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import io
import unittest
from unittest.mock import patch
from Xhpc.email import *
from Xhpc.io_utils import get_first_line

import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestEmail(unittest.TestCase):

    def setUp(self) -> None:

        self.config_folder = '%s/configs' % test_folder
        os.makedirs(self.config_folder)

        self.path1 = '%s/file1.txt' % self.config_folder
        self.path2 = '%s/file2.txt' % self.config_folder
        with open(self.path1, 'w') as o1, open(self.path2, 'w') as o2:
            o1.write('line1\nline2\nline3\n')
            o2.write('\n')
        self.valid_emails = ['this@is.ok', 'this.is@ok.too', 'and@this.ok.too']
        self.invalid_emails = ['wrong@', 'not.ok', '@try.again']

        self.path_out = '%s/created_config.txt' % self.config_folder

        self.email_fp1 = '%s/email_fp1.txt' % self.config_folder
        self.email_fp2 = '%s/email_fp2.txt' % self.config_folder
        self.email_fp3 = '%s/email_fp3.txt' % self.config_folder
        with open(self.email_fp1, 'w') as o1, open(self.email_fp2, 'w') as o2:
            o1.write('this@is.ok\n')
            o2.write('wrong@\n')

    def test_validate_email(self):
        for valid_email in self.valid_emails:
            self.assertTrue(validate_email(valid_email))
        for invalid_email in self.invalid_emails:
            self.assertFalse(validate_email(invalid_email))

    @patch('builtins.input', return_value='this@is.ok')
    def test_valid_get_email(self, mock_input):
        self.assertEqual(get_email(), 'this@is.ok')

    @patch('builtins.input', side_effect=['wrong@', 'not.ok', '@not.ok'])
    def test_invalid_get_email(self, mock_input):
        with self.assertRaises(ValueError) as cm:
            get_email()
        self.assertEqual(str(cm.exception), 'wrong@ not a valid email address')
        with self.assertRaises(ValueError) as cm:
            get_email()
        self.assertEqual(str(cm.exception), 'not.ok not a valid email address')
        with self.assertRaises(ValueError) as cm:
            get_email()
        self.assertEqual(str(cm.exception), '@not.ok not a valid email address')

    @patch('builtins.input', return_value='this@is.ok')
    def test_create_valid_config(self, input):
        create_config(self.path_out)
        first_line = get_first_line(self.path_out)
        self.assertEqual(first_line, 'this@is.ok')

    @patch('builtins.input', return_value='@not.ok')
    def test_create_invalid_config(self, input):
        with self.assertRaises(ValueError) as cm:
            create_config(self.path_out)
        self.assertEqual(str(cm.exception), '@not.ok not a valid email address')

    def test_edit_config(self):
        email = edit_config(self.path1, False)
        self.assertEqual(email, 'line1')

    @patch('builtins.input', return_value='this@is.ok')
    def test_edit_valid_config(self, input):
        email = edit_config(self.path1, True)
        self.assertEqual(email, 'this@is.ok')

    def test_write_email(self):
        with patch('sys.stdout', new=io.StringIO()) as fake_out:
            write_email(self.path_out, 'sometext')
            self.assertEqual('Written: %s\n' % self.path_out,
                             fake_out.getvalue())
            with open(self.path_out) as f:
                for line in f:
                    self.assertEqual(line.strip(), 'sometext')

    def test_get_email_address(self):
        args = {'config_email': False}
        get_email_address(args, self.path1)
        self.assertEqual(args['email_address'], 'line1')
        get_email_address(args, self.email_fp1)
        self.assertEqual(args['email_address'], 'this@is.ok')

    @patch('builtins.input', return_value='this@is.ok')
    def test_get_email_address_create(self, input):
        args = {}
        get_email_address(args, self.email_fp3)
        self.assertEqual(args['email_address'], 'this@is.ok')

    @patch('builtins.input', return_value='this@is.ok')
    def test_get_email_address_query(self, input):
        args = {'config_email': True}
        get_email_address(args, self.path1)
        self.assertEqual(args['email_address'], 'this@is.ok')
        args = {'config_email': True}
        get_email_address(args, self.email_fp2)
        self.assertEqual(args['email_address'], 'this@is.ok')

    def tearDown(self) -> None:
        for path in [
            self.path1,
            self.path2,
            self.path_out,
            self.email_fp1,
            self.email_fp2,
            self.email_fp3
        ]:
            if os.path.isfile(path):
                os.remove(path)
        for folder in [self.config_folder]:
            os.rmdir(folder)


if __name__ == '__main__':
    unittest.main()
