import unittest
from functools import lru_cache
from typing import List

import pkg_resources
from google.protobuf.json_format import MessageToJson
from google.protobuf.pyext._message import Descriptor

from arrowgen.generator import generate_for_descriptor, get_proto_module
from tests.data_generator import generate_data, generate_for_file_descriptor


@lru_cache
def _get_simple_proto_module():
    file = pkg_resources.resource_filename(__name__, "simple.proto")
    return get_proto_module(file)


def get_all_descriptors() -> List[Descriptor]:
    return list(_get_simple_proto_module().DESCRIPTOR.message_types_by_name.values())


def _prepare_data(module):
    for message in module.message_types_by_name.values():
        data = [generate_data(message, count=10) for i in range(10)]
        with open("messages/" + message.name + ".jsonl", "w") as fp:
            for d in data:
                fp.write(MessageToJson(d, indent=0).replace("\n", ""))
                fp.write("\n")


class TestDataGen(unittest.TestCase):
    def test_generate_some(self):
        simple = _get_simple_proto_module()
        generate_data(simple.SearchRequest.DESCRIPTOR, 10)
        generate_data(simple.DataRow.DESCRIPTOR, 10)
        generate_data(simple.OneofMessage.DESCRIPTOR, 10)


class GeneratorTest(unittest.TestCase):
    def test_generate(self):
        simple = _get_simple_proto_module()
        generate_for_file_descriptor(simple.DESCRIPTOR, "./messages", 10)
        files = generate_for_descriptor(simple.DESCRIPTOR)

    def test_get_all_descriptors(self):
        self.assertGreater(len(get_all_descriptors()), 6)
