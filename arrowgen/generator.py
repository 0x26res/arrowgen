import pkgutil
from typing import Tuple

from google.protobuf.descriptor import FileDescriptor
from jinja2 import Template

from arrowgen.wrappers import FileWrapper


def generate_for_descriptor(file_descriptor: FileDescriptor) -> Tuple[str, str]:
    wrapper = FileWrapper(file_descriptor)

    header_template = pkgutil.get_data(__name__, 'templates/appender.h').decode('utf-8')
    header = Template(header_template).render(file_wrapper=wrapper)
    with(open(wrapper.appender_header(), 'w')) as fp:
        fp.write(header)

    source_template = pkgutil.get_data(__name__, 'templates/appender.cc').decode('utf-8')
    source = Template(source_template).render(file_wrapper=wrapper)
    with(open(wrapper.appender_source(), 'w')) as fp:
        fp.write(source)
    return wrapper.appender_header(), wrapper.appender_source()


def generate_for_file(proto_file):
    pass
