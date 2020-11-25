import importlib
from unittest import TestCase

from google.protobuf.json_format import MessageToJson

from arrowgen.generator import generate_for_descriptor
from tests.messages.simple_pb2 import SearchRequest, DataRow
from tests.test_utils import generate_data
from arrowgen.utils import run_command


class TestDataGen(TestCase):
    def test_some(self):
        generate_data(SearchRequest)
        generate_data(DataRow)


def _prepare_data(module):
    for message in module.message_types_by_name.values():
        data = [generate_data(message) for i in range(10)]
        with open('messages/' + message.name + '.jsonl', 'w') as fp:
            for d in data:
                fp.write(MessageToJson(d, indent=0).replace('\n', ''))
                fp.write('\n')


class TestGenerator(TestCase):

    def test_generate(self):
        run_command(['protoc', '-I=./', './messages/simple.proto', '--cpp_out=./', '--python_out=./'])
        simple = importlib.import_module("tests.messages.simple_pb2")
        _prepare_data(simple.DESCRIPTOR)
        header, source = generate_for_descriptor(simple.DESCRIPTOR)
        run_command([
            'g++',
            '-g',
            '-I./',
            'simple_tester.cc',
            source,
            'messages/simple.pb.cc',
            '/usr/lib/x86_64-linux-gnu/libarrow.so.200',
            '/usr/lib/x86_64-linux-gnu/libprotobuf.so'
        ])
        run_command(['./a.out'])
