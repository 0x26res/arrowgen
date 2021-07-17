import unittest

from google.protobuf.json_format import MessageToJson

from arrowgen.generator import generate_for_descriptor
from tests.data_generator import generate_message, generate_for_file_descriptor
from tests.test_utils import get_all_descriptors, _get_simple_proto_module


def _prepare_data(module):
    for message in module.message_types_by_name.values():
        data = [generate_message(message, count=10) for i in range(10)]
        with open("messages/" + message.name + ".jsonl", "w") as fp:
            for d in data:
                fp.write(MessageToJson(d, indent=0).replace("\n", ""))
                fp.write("\n")


class TestDataGen(unittest.TestCase):
    def test_generate_some(self):
        simple = _get_simple_proto_module()
        generate_message(simple.SearchRequest.DESCRIPTOR, 10)
        generate_message(simple.DataRow.DESCRIPTOR, 10)
        generate_message(simple.OneofMessage.DESCRIPTOR, 10)

    def test_generate(self):
        simple = _get_simple_proto_module()
        generate_for_file_descriptor(simple.DESCRIPTOR, "./messages", 10)
        files = generate_for_descriptor(simple.DESCRIPTOR)

    def test_get_all_descriptors(self):
        self.assertGreater(len(get_all_descriptors()), 6)

    @unittest.skip("Skip until optionals are in")
    def test_optional(self):
        descriptor = _get_simple_proto_module().WithOptionalMessage.DESCRIPTOR
        while generate_message(descriptor, 10).HasField("optional_int"):
            # Eventually we should have an optional
            pass
