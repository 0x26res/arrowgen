import importlib
import pathlib
import pkgutil
import subprocess
import tempfile
from typing import Dict, Tuple

from google.protobuf.descriptor import FileDescriptor
from jinja2 import Template

from arrowgen.utils import run_command
from arrowgen.wrappers import FileWrapper
import os


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


def write_files(content: Dict[str, str]) -> Tuple[str, str]:
    for file_name, file_content in content.items():
        with (open(file_name, "w")) as fp:
            fp.write(file_content)
    return tuple(content.keys())


def get_file_descriptor(proto_file: str) -> FileDescriptor:
    with tempfile.TemporaryDirectory() as tempdir:
        include = pathlib.Path(proto_file).parent.as_posix()
        run_command(
            [
                "protoc",
                "--proto_path=" + include,
                proto_file,
                "--python_out=" + tempdir,
            ]
        )
        python_file = os.path.join(
            tempdir, os.path.basename(proto_file[:-6]) + "_pb2.py"
        )
        spec = importlib.util.spec_from_file_location("module.name", python_file)
        proto_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(proto_module)
        return proto_module.DESCRIPTOR


def generate_for_file(proto_file: str) -> Tuple[str, str]:
    file_descriptor = get_file_descriptor(proto_file)
    return write_files(generate_for_descriptor(file_descriptor))
