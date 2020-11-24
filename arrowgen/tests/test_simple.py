import importlib
from unittest import TestCase

from google.protobuf.json_format import MessageToJson

from arrowgen.generator import generate_for_descriptor
from arrowgen.tests.messages.simple_pb2 import SearchRequest, DataRow
from arrowgen.tests.test_utils import run_command, generate_data


class TestDataGen(TestCase):
    def test_some(self):
        generate_data(SearchRequest)
        generate_data(DataRow)


class TestGenerator(TestCase):

    def _prepare_data(self, module):
        for message in module.message_types_by_name.values():
            data = [generate_data(message) for i in range(10)]
            with open('messages/' + message.name + '.jsonl', 'w') as fp:
                for d in data:
                    fp.write(MessageToJson(d, indent=0).replace('\n', ''))
                    fp.write('\n')


    def test_generate(self):
        run_command(['protoc', '-I=./', './messages/simple.proto', '--cpp_out=./', '--python_out=./'])
        simple = importlib.import_module("arrowgen.tests.messages.simple_pb2")
        self._prepare_data(simple.DESCRIPTOR)
        header, source = generate_for_descriptor(simple.DESCRIPTOR)
        run_command([
            'g++',
            '-g',
            '-I./',
            'simple_tester.cc',
            'messages/simple.appender.cc',
            'messages/simple.pb.cc',
            '/usr/lib/x86_64-linux-gnu/libarrow.so.200',
            '/usr/lib/x86_64-linux-gnu/libprotobuf.so'
        ])
        run_command(['./a.out'])
