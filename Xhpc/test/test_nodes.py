# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from Xhpc.nodes import *
import pkg_resources
test_folder = pkg_resources.resource_filename("Xhpc", "test")


class TestGetNodesPpn(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_get_nodes_ppn(self):
        get_nodes_ppn()

    def tearDown(self) -> None:
        pass


class TestGetNodelist(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_get_nodelist(self):
        get_nodelist()

    def tearDown(self) -> None:
        pass


class TestAllocateNodes(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_allocate_nodes(self):
        allocate_nodes()

    def tearDown(self) -> None:
        pass


if __name__ == '__main__':
    unittest.main()
