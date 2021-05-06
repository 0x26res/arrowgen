import typing

import pyarrow
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message

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
        result = pyarrow.struct(get_arrow_fields(descriptor.message_type))
    else:
        result = ARROW_TYPES[descriptor.cpp_type]
    if descriptor.label == FieldDescriptor.LABEL_REPEATED:
        return pyarrow.list_(result)
    else:
        return result


def get_arrow_field(descriptor: FieldDescriptor) -> pyarrow.Field:
    arrow_type = get_arrow_type(descriptor)
    return pyarrow.field(descriptor.name, arrow_type)


def get_arrow_fields(descriptor: Descriptor) -> typing.List[pyarrow.Field]:
    return [get_arrow_field(field) for field in descriptor.fields]


def get_arrow_schema(descriptor: Descriptor) -> pyarrow.Schema:
    return pyarrow.schema(get_arrow_fields(descriptor))


def calculate_offsets(
    list_of_list: typing.List[typing.List[typing.Any]],
) -> typing.List[int]:
    results = [0]
    current = 0
    for value in list_of_list:
        current += len(value)
        results.append(current)
    return results


def _extract_field_for_tuple(message, field_descriptor: FieldDescriptor):
    if field_descriptor.containing_oneof is not None and not message.HasField(
        field_descriptor.name
    ):
        return None
    else:
        value = getattr(message, field_descriptor.name)
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
                return [
                    message_to_tuple(v, field_descriptor.message_type) for v in value
                ]
            else:
                return list(value)
        elif field_descriptor.message_type:
            return message_to_tuple(value, field_descriptor.message_type)
        else:
            return value


def message_to_tuple(
    message: Message, descriptor: Descriptor
) -> typing.Optional[typing.Tuple]:
    if message is None:
        return None
    else:
        return tuple(
            _extract_field_for_tuple(message, field_descriptor)
            for field_descriptor in descriptor.fields
        )


def get_field_array(
    messages: typing.List[Message], descriptor: FieldDescriptor
) -> pyarrow.Array:
    if descriptor.containing_oneof is not None:
        values = [
            getattr(message, descriptor.name)
            if message is not None and message.HasField(descriptor.name)
            else None
            for message in messages
        ]
    else:
        values = [
            getattr(message, descriptor.name) if message else None
            for message in messages
        ]

    if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        if descriptor.label == FieldDescriptor.LABEL_REPEATED:
            offsets = calculate_offsets(values)
            return pyarrow.ListArray.from_arrays(
                offsets,
                pyarrow.array(
                    [
                        message_to_tuple(item, descriptor.message_type)
                        for sublist in values
                        for item in sublist
                    ],
                    type=pyarrow.struct(get_arrow_fields(descriptor.message_type)),
                ),
            )
        else:
            messages_tuples = [
                message_to_tuple(value, descriptor.message_type) for value in values
            ]
            return pyarrow.array(
                messages_tuples,
                type=pyarrow.struct(get_arrow_fields(descriptor.message_type)),
            )
    else:
        if descriptor.label == FieldDescriptor.LABEL_REPEATED:
            values = [list(value) for value in values]
        return pyarrow.array(values, get_arrow_type(descriptor))


def messages_to_arrays(
    messages: typing.List[Message], descriptor: Descriptor
) -> typing.List[pyarrow.Array]:
    return [get_field_array(messages, field) for field in descriptor.fields]


def messages_to_table(
    messages: typing.List[Message], descriptor: Descriptor
) -> pyarrow.Table:
    arrays = messages_to_arrays(messages, descriptor)
    return pyarrow.Table.from_arrays(arrays, schema=get_arrow_schema(descriptor))
