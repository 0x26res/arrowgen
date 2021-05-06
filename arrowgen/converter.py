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
    FieldDescriptor.TYPE_BYTES: pyarrow.binary(),
    FieldDescriptor.TYPE_UINT32: pyarrow.uint32(),
    FieldDescriptor.TYPE_ENUM: pyarrow.int32(),
    FieldDescriptor.TYPE_SFIXED32: pyarrow.int32(),
    FieldDescriptor.TYPE_SFIXED64: pyarrow.int64(),
    FieldDescriptor.TYPE_SINT32: pyarrow.int32(),
    FieldDescriptor.TYPE_SINT64: pyarrow.int64(),
}


def get_arrow_type(field_descriptor: FieldDescriptor) -> pyarrow.DataType:
    if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
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


def _extract_field_for_tuple(
    message: Message, field_descriptor: FieldDescriptor
) -> typing.Any:
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
    message: Message, message_descriptor: Descriptor
) -> typing.Optional[typing.Tuple]:
    if message is None:
        return None
    else:
        return tuple(
            _extract_field_for_tuple(message, field_descriptor)
            for field_descriptor in message_descriptor.fields
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

    if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            offsets = calculate_offsets(values)
            return pyarrow.ListArray.from_arrays(
                offsets,
                pyarrow.array(
                    [
                        message_to_tuple(item, field_descriptor.message_type)
                        for sublist in values
                        for item in sublist
                    ],
                    type=pyarrow.struct(
                        get_arrow_fields(field_descriptor.message_type)
                    ),
                ),
            )
        else:
            messages_tuples = [
                message_to_tuple(value, field_descriptor.message_type)
                for value in values
            ]
            return pyarrow.array(
                messages_tuples,
                type=pyarrow.struct(get_arrow_fields(field_descriptor.message_type)),
            )
    else:
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            values = [list(value) for value in values]
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


def extract_field(
    array: pyarrow.Array,
    field_descriptor: FieldDescriptor,
    messages: typing.List[Message],
):
    valid = array.is_valid()
    for i, message in enumerate(messages):
        if valid[i]:
            scalar = array[i]
            # TODO: delete
            # print(type(scalar), scalar, field_descriptor.name)
            field_value = convert_scalar(scalar, field_descriptor)
            if field_descriptor.message_type or field_descriptor.containing_oneof:
                # TODO: add supported for nested messages
                pass
            elif field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
                getattr(message, field_descriptor.name).extend(field_value)
            else:
                setattr(message, field_descriptor.name, field_value)


def table_to_messages(
    table: pyarrow.Table, message_descriptor: Descriptor
) -> typing.List[Message]:
    messages = [message_descriptor._concrete_class() for _ in range(table.num_rows)]
    for field_descriptor in message_descriptor.fields:
        extract_field(table[field_descriptor.name], field_descriptor, messages)
    return messages
