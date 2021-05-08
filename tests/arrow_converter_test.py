import unittest

import pyarrow

from arrowgen.arrow_converter import messages_to_table, get_arrow_schema
from tests.data_generator import generate_messages
from tests.generator_test import get_all_descriptors


class SchemaConverterTest(unittest.TestCase):
    def test_get_arrow_schema(self):
        for descriptor in get_all_descriptors():
            self.assertIsInstance(get_arrow_schema(descriptor), pyarrow.Schema)


class MessageConverterTest(unittest.TestCase):
    def test_all_messages(self):
        for message_descriptor in get_all_descriptors():
            print(message_descriptor.name)
            messages = generate_messages(message_descriptor, 20)
            table = messages_to_table(messages, message_descriptor)
            self.assertIsInstance(table, pyarrow.Table)
            self.assertEqual(len(table), 20)
            # messages_back = table_to_messages(table, message_descriptor)
