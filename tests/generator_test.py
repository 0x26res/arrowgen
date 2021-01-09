import importlib
import unittest

from google.protobuf.json_format import MessageToJson

from arrowgen.generator import generate_for_descriptor, write_files
from arrowgen.utils import run_command
from tests.data_generator import generate_data, generate_for_file_descriptor


def _get_simple_proto_module():
    run_command(
        [
            "protoc",
            "--proto_path=./",
            "./messages/simple.proto",
            "--cpp_out=./",
            "--python_out=./",
        ]
    )
    return importlib.import_module("tests.messages.simple_pb2")


def _prepare_data(module):
    for message in module.message_types_by_name.values():
        data = [generate_data(message) for i in range(10)]
        with open("messages/" + message.name + ".jsonl", "w") as fp:
            for d in data:
                fp.write(MessageToJson(d, indent=0).replace("\n", ""))
                fp.write("\n")


class TestDataGen(unittest.TestCase):
    def test_generate_some(self):
        simple = _get_simple_proto_module()
        generate_data(simple.SearchRequest.DESCRIPTOR)
        generate_data(simple.DataRow.DESCRIPTOR)


class GeneratorTest(unittest.TestCase):
    def test_generate(self):
        simple = _get_simple_proto_module()
        generate_for_file_descriptor(simple.DESCRIPTOR, "./messages", 10)
        files = generate_for_descriptor(simple.DESCRIPTOR)
