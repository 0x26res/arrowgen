import importlib
import pkgutil
import subprocess
from typing import Dict, Tuple

from google.protobuf.descriptor import FileDescriptor
from jinja2 import Template

from arrowgen.utils import run_command
from arrowgen.wrappers import FileWrapper


def clang_format(code: str) -> str:
    results = subprocess.run(
        ["clang-format"],
        input=code,
        text=True,
        capture_output=True,
    )
    if results.returncode != 0:
        raise RuntimeError(results.stderr)
    else:
        return results.stdout


def generate_for_descriptor(file_descriptor: FileDescriptor) -> Dict[str, str]:
    """
    Generates the arrow appender and returns the file names and content
    """

    wrapper = FileWrapper(file_descriptor)

    header_template = pkgutil.get_data(__name__, "templates/arrow.h").decode("utf-8")
    header = Template(header_template).render(file_wrapper=wrapper)

    source_template = pkgutil.get_data(__name__, "templates/arrow.cc").decode("utf-8")
    source = Template(source_template).render(file_wrapper=wrapper)
    # TODO: format in place

    return {
        wrapper.appender_header(): clang_format(header),
        wrapper.appender_source(): clang_format(source),
    }


def write_files(content: Dict[str, str]) -> Tuple[str]:
    for file_name, file_content in content.items():
        with (open(file_name, "w")) as fp:
            fp.write(file_content)
    return tuple(content.keys())


def generate_for_file(proto_file: str) -> Tuple[str]:
    run_command(["protoc", "-I=./", proto_file, "--cpp_out=./", "--python_out=./"])
    python_file = proto_file[:-6] + "_pb2.py"
    spec = importlib.util.spec_from_file_location("module.name", python_file)
    proto_module = importlib.util.module_from_spec(spec)
    return write_files(generate_for_descriptor(proto_module.DESCRIPTOR))
