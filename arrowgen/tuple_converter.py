"""
Converts protobuf messages to plain tuples
"""

import typing

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message


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
