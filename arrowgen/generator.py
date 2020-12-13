import importlib
import pkgutil
from typing import Dict, Tuple

from google.protobuf.descriptor import FileDescriptor
from jinja2 import Template

from arrowgen.utils import run_command
from arrowgen.wrappers import FileWrapper


def generate_for_descriptor(file_descriptor: FileDescriptor) -> Dict[str, str]:
    """
    Generates the arrow appender and returns the file names and content
    """

    wrapper = FileWrapper(file_descriptor)

    header_template = pkgutil.get_data(__name__, 'templates/appender.h').decode('utf-8')
    header = Template(header_template).render(file_wrapper=wrapper)
    with(open(wrapper.appender_header(), 'w')) as fp:
        fp.write(header)

    source_template = pkgutil.get_data(__name__, 'templates/appender.cc').decode('utf-8')
    source = Template(source_template).render(file_wrapper=wrapper)
    with(open(wrapper.appender_source(), 'w')) as fp:
        fp.write(source)
    # run_command(['clang-format', '-i', wrapper.appender_header(), wrapper.appender_source()])
    return {
        wrapper.appender_header(): header,
        wrapper.appender_source(): source
    }


def write_files(content: Dict[str, str]) -> Tuple[str]:
    for file_name, file_content in content.items():
        with(open(file_name, 'w')) as fp:
            fp.write(file_content)
    return tuple(content.keys())


def generate_for_file(proto_file: str) -> Tuple[str]:
    run_command(['protoc', '-I=./', proto_file, '--cpp_out=./', '--python_out=./'])
    python_file = proto_file[:-6] + '_pb2.py'
    spec = importlib.util.spec_from_file_location("module.name", python_file)
    proto_module = importlib.util.module_from_spec(spec)
    return write_files(generate_for_descriptor(proto_module.DESCRIPTOR))
