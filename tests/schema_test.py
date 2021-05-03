import unittest
from typing import List

import pyarrow
from google.protobuf.descriptor import Descriptor, FieldDescriptor, OneofDescriptor

from tests.generator_test import get_all_descriptors

ARROW_TYPES = {
    FieldDescriptor.TYPE_DOUBLE: pyarrow.float64(),
    FieldDescriptor.TYPE_FLOAT: pyarrow.float32(),
    FieldDescriptor.TYPE_INT64: pyarrow.int64(),
    FieldDescriptor.TYPE_UINT64: pyarrow.uint64(),
    FieldDescriptor.TYPE_INT32: pyarrow.int32(),
    FieldDescriptor.TYPE_FIXED64: pyarrow.uint64(),
    FieldDescriptor.TYPE_FIXED32: pyarrow.uint32(),
    FieldDescriptor.TYPE_BOOL: pyarrow.bool_(),
    FieldDescriptor.TYPE_STRING: pyarrow.utf8(),
    FieldDescriptor.TYPE_BYTES: pyarrow.string(),
    FieldDescriptor.TYPE_UINT32: pyarrow.uint32(),
    FieldDescriptor.TYPE_ENUM: pyarrow.int32(),
    FieldDescriptor.TYPE_SFIXED32: pyarrow.int32(),
    FieldDescriptor.TYPE_SFIXED64: pyarrow.int64(),
    FieldDescriptor.TYPE_SINT32: pyarrow.int32(),
    FieldDescriptor.TYPE_SINT64: pyarrow.int64(),
    FieldDescriptor.CPPTYPE_BOOL: pyarrow.bool_(),
    FieldDescriptor.CPPTYPE_DOUBLE: pyarrow.float64(),
    FieldDescriptor.CPPTYPE_ENUM: pyarrow.int32(),
    FieldDescriptor.CPPTYPE_FLOAT: pyarrow.float32(),
    FieldDescriptor.CPPTYPE_INT32: pyarrow.int32(),
    FieldDescriptor.CPPTYPE_INT64: pyarrow.int64(),
    FieldDescriptor.CPPTYPE_STRING: pyarrow.utf8(),
    FieldDescriptor.CPPTYPE_UINT32: pyarrow.uint32(),
    FieldDescriptor.CPPTYPE_UINT64: pyarrow.int64(),
}


def get_arrow_type(descriptor: FieldDescriptor) -> pyarrow.DataType:
    if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        return pyarrow.struct(get_arrow_fields(descriptor.message_type))
    else:
        return ARROW_TYPES[descriptor.cpp_type]


def get_arrow_field(descriptor: FieldDescriptor) -> pyarrow.Field:
    arrow_type = get_arrow_type(descriptor)
    if descriptor.label == FieldDescriptor.LABEL_REPEATED:
        arrow_type = pyarrow.list_(arrow_type)
    return pyarrow.field(descriptor.name, arrow_type)


def get_oneof(descriptor: OneofDescriptor) -> pyarrow.Field:
    children = [get_arrow_field(field) for field in descriptor.fields]
    return pyarrow.field(descriptor.name, pyarrow.union(children, "dense"))


def get_oneofs(descriptor: Descriptor) -> List[pyarrow.Field]:
    return [get_oneof(oneof_descriptor) for oneof_descriptor in descriptor.oneofs]


def get_arrow_fields(descriptor: Descriptor) -> List[pyarrow.Field]:
    free_fields = [
        get_arrow_field(field)
        for field in descriptor.fields
        if field.containing_oneof is None
    ]
    oneofs = get_oneofs(descriptor)
    return free_fields + oneofs


def get_arrow_schema(descriptor: Descriptor) -> pyarrow.Schema:
    return pyarrow.schema(get_arrow_fields(descriptor))


class SchemaTest(unittest.TestCase):
    def test_get_arrow_schema(self):
        for descriptor in get_all_descriptors():
            self.assertIsInstance(get_arrow_schema(descriptor), pyarrow.Schema)
