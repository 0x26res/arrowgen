import typing

import google.protobuf.timestamp_pb2
import pandas
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
    FieldDescriptor.TYPE_BYTES: pyarrow.binary(),
    FieldDescriptor.TYPE_UINT32: pyarrow.uint32(),
    FieldDescriptor.TYPE_ENUM: pyarrow.int32(),
    FieldDescriptor.TYPE_SFIXED32: pyarrow.int32(),
    FieldDescriptor.TYPE_SFIXED64: pyarrow.int64(),
    FieldDescriptor.TYPE_SINT32: pyarrow.int32(),
    FieldDescriptor.TYPE_SINT64: pyarrow.int64(),
}


def is_timestamp(field_descriptor: FieldDescriptor) -> bool:
    return (
        field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
        and field_descriptor.message_type.full_name == "google.protobuf.Timestamp"
    )


def get_arrow_type(field_descriptor: FieldDescriptor) -> pyarrow.DataType:
    if is_timestamp(field_descriptor):
        result = pyarrow.timestamp("ns")
    elif field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        result = pyarrow.struct(get_arrow_fields(field_descriptor.message_type))
    else:
        result = ARROW_TYPES[field_descriptor.type]
    if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
        return pyarrow.list_(result)
    else:
        return result


def get_arrow_field(field_descriptor: FieldDescriptor) -> pyarrow.Field:
    arrow_type = get_arrow_type(field_descriptor)
    return pyarrow.field(field_descriptor.name, arrow_type)


def get_arrow_fields(message_descriptor: Descriptor) -> typing.List[pyarrow.Field]:
    return [get_arrow_field(field) for field in message_descriptor.fields]


def get_arrow_schema(message_descriptor: Descriptor) -> pyarrow.Schema:
    return pyarrow.schema(get_arrow_fields(message_descriptor))


def calculate_offsets(
    list_of_list: typing.List[typing.List[typing.Any]],
) -> typing.List[int]:
    results = [0]
    current = 0
    for value in list_of_list:
        current += len(value)
        results.append(current)
    return results


def messages_to_struct_array(
    messages: typing.List[Message], message_descriptor: Descriptor
) -> pyarrow.StructArray:
    arrays = messages_to_arrays(messages, message_descriptor)
    validity_mask = pyarrow.array(value is not None for value in messages)
    validity_bitmask = validity_mask.buffers()[1]
    return pyarrow.StructArray.from_buffers(
        pyarrow.struct(get_arrow_fields(message_descriptor)),
        len(messages),
        [validity_bitmask],
        children=arrays,
    )


def repeated_messages_to_list_array(
    messages: typing.List[typing.List[Message]], message_descriptor: Descriptor
) -> pyarrow.ListArray:
    flat_messages = [item for sublist in messages if sublist for item in sublist]
    struct_array = messages_to_struct_array(flat_messages, message_descriptor)
    offsets = calculate_offsets(messages)
    buffers = [
        pyarrow.array(value is not None for value in messages).buffers()[1],
        pyarrow.array(offsets, pyarrow.int32()).buffers()[1],
    ]
    return pyarrow.ListArray.from_buffers(
        type=pyarrow.list_(struct_array.type),
        length=len(messages),
        buffers=buffers,
        children=[struct_array],
    )


def convert_timestamp_to_ns(
    timestamps: typing.List[google.protobuf.timestamp_pb2.Timestamp],
) -> typing.List[int]:
    return [
        timestamp.seconds * 1_000_000_000 + timestamp.nanos for timestamp in timestamps
    ]


def convert_to_proto_timestamp(
    pd_timestamp: pandas.Timestamp,
) -> google.protobuf.timestamp_pb2.Timestamp:
    nanos = pd_timestamp.value
    return google.protobuf.timestamp_pb2.Timestamp(
        seconds=nanos // 1_000_000_000, nanos=nanos % 1_000_000_000
    )


def get_field_array(
    messages: typing.List[Message], field_descriptor: FieldDescriptor
) -> pyarrow.Array:
    if field_descriptor.containing_oneof is not None:
        values = [
            getattr(message, field_descriptor.name)
            if message is not None and message.HasField(field_descriptor.name)
            else None
            for message in messages
        ]
    else:
        values = [
            getattr(message, field_descriptor.name) if message else None
            for message in messages
        ]

    if (
        not is_timestamp(field_descriptor)
        and field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
    ):
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            return repeated_messages_to_list_array(
                values, field_descriptor.message_type
            )
        else:
            return messages_to_struct_array(values, field_descriptor.message_type)
    else:
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            if is_timestamp(field_descriptor):
                values = [convert_timestamp_to_ns(value) for value in values]
            else:
                values = [list(value) for value in values]
        elif is_timestamp(field_descriptor):
            values = convert_timestamp_to_ns(values)
        return pyarrow.array(values, get_arrow_type(field_descriptor))


def messages_to_arrays(
    messages: typing.List[Message], message_descriptor: Descriptor
) -> typing.List[pyarrow.Array]:
    return [get_field_array(messages, field) for field in message_descriptor.fields]


def messages_to_table(
    messages: typing.List[Message], message_descriptor: Descriptor
) -> pyarrow.Table:
    arrays = messages_to_arrays(messages, message_descriptor)
    return pyarrow.Table.from_arrays(
        arrays, schema=get_arrow_schema(message_descriptor)
    )


def convert_scalar(scalar, field_descriptor: FieldDescriptor):
    return scalar.as_py()


def struct_array_to_messages(
    struct_array: pyarrow.StructArray, message_descriptor: Descriptor
) -> typing.List[Message]:
    assert len(struct_array.type) == len(message_descriptor.fields)
    valid = struct_array.is_valid()
    messages = [
        message_descriptor._concrete_class() if is_valid.as_py() else None
        for is_valid in valid
    ]
    for i, field_descriptor in enumerate(message_descriptor.fields):
        extract_field(struct_array.field(i), field_descriptor, messages)
    return messages


def chunked_array_to_messages(
    chunked_array: pyarrow.ChunkedArray, message_descriptor: Descriptor
) -> typing.List[Message]:
    if isinstance(chunked_array, pyarrow.ChunkedArray):
        results = []
        for chunk in chunked_array.chunks:
            results.extend(struct_array_to_messages(chunk, message_descriptor))
        return results
    else:
        assert isinstance(chunked_array, pyarrow.StructArray)
        return struct_array_to_messages(chunked_array, message_descriptor)


def list_array_to_list_of_messages(
    list_array: pyarrow.ListArray, message_descriptor: Descriptor
) -> typing.List[typing.List[Message]]:
    values_view = list_array.values
    values = struct_array_to_messages(values_view, message_descriptor)
    offsets = list_array.offsets
    is_valid = list_array.is_valid()
    results = []
    for i in range(len(list_array)):
        if is_valid[i].as_py():
            results.append(values[offsets[i].as_py() : offsets[i + 1].as_py()])
        else:
            results.append(None)
    return results


def chunked_array_to_list_of_messages(
    chunked_array: pyarrow.ChunkedArray, message_descriptor: Descriptor
) -> typing.List[typing.List[Message]]:
    if isinstance(chunked_array, pyarrow.ChunkedArray):
        results = []
        for chunk in chunked_array.chunks:
            results.extend(list_array_to_list_of_messages(chunk, message_descriptor))
        return results
    else:
        assert isinstance(chunked_array, pyarrow.ListArray)
        return list_array_to_list_of_messages(chunked_array, message_descriptor)


def assign_values(
    messages: typing.List[Message],
    values: typing.List[typing.Any],
    field_descriptor: FieldDescriptor,
):
    assert len(messages) == len(values)
    if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
        for message, value in zip(messages, values):
            if message is not None and value is not None:
                if is_timestamp(field_descriptor):
                    value = [convert_to_proto_timestamp(v) for v in value]
                getattr(message, field_descriptor.name).extend(value)
    elif field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        for message, value in zip(messages, values):
            if message is not None and value is not None:
                if is_timestamp(field_descriptor):
                    value = convert_to_proto_timestamp(value)

                getattr(message, field_descriptor.name).CopyFrom(value)
    else:
        for message, value in zip(messages, values):
            if message is not None and value is not None:
                setattr(message, field_descriptor.name, value)


def extract_field(
    array: pyarrow.Array,
    field_descriptor: FieldDescriptor,
    messages: typing.List[Message],
):
    if (
        not is_timestamp(field_descriptor)
        and field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
    ):
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            values = chunked_array_to_list_of_messages(
                array, field_descriptor.message_type
            )
        else:
            values = chunked_array_to_messages(array, field_descriptor.message_type)
    else:
        valid_array = array.is_valid()
        values = [
            convert_scalar(array[i], field_descriptor) if valid else None
            for i, valid in enumerate(valid_array)
        ]
    assign_values(messages, values, field_descriptor)


def table_to_messages(
    table: pyarrow.Table, message_descriptor: Descriptor
) -> typing.List[Message]:
    messages = [message_descriptor._concrete_class() for _ in range(table.num_rows)]
    for field_descriptor in message_descriptor.fields:
        extract_field(table[field_descriptor.name], field_descriptor, messages)
    return messages
