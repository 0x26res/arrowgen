import typing
import unittest

import pyarrow
from google.protobuf import json_format, text_format
from google.protobuf.message import Message

from arrowgen import arrow_converter
from tests.data_generator import generate_messages
from tests.generator_test import get_all_descriptors


class SchemaConverterTest(unittest.TestCase):
    def test_get_arrow_schema(self):
        for descriptor in get_all_descriptors():
            self.assertIsInstance(
                arrow_converter.get_arrow_schema(descriptor), pyarrow.Schema
            )


class MessageConverterTest(unittest.TestCase):
    def test_all_messages(self):
        for message_descriptor in get_all_descriptors():
            messages = generate_messages(message_descriptor, 20)
            table = arrow_converter.messages_to_table(messages, message_descriptor)
            self.assertIsInstance(table, pyarrow.Table)
            self.assertEqual(len(table), 20)
            messages_back = arrow_converter.table_to_messages(table, message_descriptor)
            # print(table.to_pandas().to_markdown())
            # for m, mb in zip(messages, messages_back):
            #     print(text_format.MessageToString(m, as_one_line=True))
            #     print(text_format.MessageToString(mb, as_one_line=True))
            self.assertMessagesEqual(messages, messages_back, message_descriptor.name)

    def assertMessagesEqual(
        self, left: typing.List[Message], right: typing.List[Message], message
    ):
        self.assertEqual(len(left), len(right))
        for left_message, right_message in zip(left, right):
            self.assertMessageEqual(left_message, right_message, message)

    def assertMessageEqual(self, left: Message, right: Message, message):
        left_payload = json_format.MessageToJson(left, preserving_proto_field_name=True)
        right_payload = json_format.MessageToJson(
            right, preserving_proto_field_name=True
        )
        self.assertEqual(left_payload, right_payload, message)
