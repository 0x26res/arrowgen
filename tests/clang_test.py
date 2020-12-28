from unittest.case import TestCase

from arrowgen.generator import clang_format


class ClangFormatTest(TestCase):
    def test_clang(self):
        results = clang_format('#include    "foo.h"')
        self.assertEqual('#include "foo.h"', results)

    def test_clang_bad(self):
        results = clang_format("!!! this is not C++ \n\n\n\n")
        self.assertEqual("!!!this is not C++\n", results)
