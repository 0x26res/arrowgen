import importlib
import subprocess
from unittest import TestCase

from arrowgen.generator import generate_for_descriptor


def run_command(command):
    subprocess.run(command, check=True, text=True)


class TestGenerator(TestCase):

    def test_generate(self):
        run_command(['protoc',  '-I=./', './messages/simple.proto', '--cpp_out=./', '--python_out=./'])
        simple = importlib.import_module("arrowgen.tests.messages.simple_pb2")
        header, source = generate_for_descriptor(simple.DESCRIPTOR)
        run_command([
            'g++',
            '-I./',
            'simple_tester.cc',
            'messages/simple.appender.cc',
            'messages/simple.pb.cc',
            '/usr/lib/x86_64-linux-gnu/libarrow.so.200',
            '/usr/lib/x86_64-linux-gnu/libprotobuf.so'
        ])
        run_command(['./a.out'])
