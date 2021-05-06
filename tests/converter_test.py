import unittest

import pyarrow

from arrowgen.converter import messages_to_table, get_arrow_schema
from tests.data_generator import generate_messages
from tests.generator_test import get_all_descriptors


class SchemaConverterTest(unittest.TestCase):
    def test_get_arrow_schema(self):
        for descriptor in get_all_descriptors():
            self.assertIsInstance(get_arrow_schema(descriptor), pyarrow.Schema)


class MessageConverterTest(unittest.TestCase):
    def test_all_messages(self):
        for message_descriptor in get_all_descriptors():
            messages = generate_messages(message_descriptor, 20)
            table = messages_to_table(messages, message_descriptor)
            self.assertIsInstance(table, pyarrow.Table)
            self.assertEqual(len(table), 20)


class LearningTest(unittest.TestCase):
    def test_offset_behavior(self):
        array = pyarrow.ListArray.from_arrays([0, 1, 6], [0, 1, 2, 3, 4, 5, 6])
        self.assertEqual(len(array), 2)
        self.assertEqual(len(array[0]), 1)
        self.assertEqual(len(array[1]), 5)

    def test_struct_array_from_array_behavior(self):
        array = pyarrow.StructArray.from_arrays(
            [[None, 1], [None, "foo"]],
            fields=[
                pyarrow.field("col1", pyarrow.int64()),
                pyarrow.field("col2", pyarrow.string()),
            ],
        )
        self.assertEqual(array.null_count, 0)

    def test_struct_array_from_tuple_behavior(self):
        array = pyarrow.array(
            [
                None,
                (1, "foo"),
            ],
            type=pyarrow.struct(
                [
                    pyarrow.field("col1", pyarrow.int64()),
                    pyarrow.field("col2", pyarrow.string()),
                ]
            ),
        )
        self.assertEqual(array.null_count, 1)
