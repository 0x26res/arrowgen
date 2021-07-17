import functools
import typing

import pkg_resources
from google.protobuf.descriptor import Descriptor

from arrowgen.generator import get_proto_module


@functools.lru_cache
def _get_simple_proto_module():
    file = pkg_resources.resource_filename(__name__, "simple.proto")
    return get_proto_module(file)


def get_all_descriptors() -> typing.List[Descriptor]:
    return list(_get_simple_proto_module().DESCRIPTOR.message_types_by_name.values())


def get_descriptor(name: str) -> Descriptor:
    return _get_simple_proto_module().DESCRIPTOR.message_types_by_name[name]
