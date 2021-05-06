import unittest
from typing import List

import pyarrow
from google.protobuf.descriptor import Descriptor, FieldDescriptor, OneofDescriptor

from arrowgen.converter import get_arrow_schema
from tests.generator_test import get_all_descriptors


class SchemaTest(unittest.TestCase):
    def test_get_arrow_schema(self):
        for descriptor in get_all_descriptors():
            self.assertIsInstance(get_arrow_schema(descriptor), pyarrow.Schema)
