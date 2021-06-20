import argparse
import os
import pathlib
import random
import typing

from google.protobuf.descriptor import (
    FieldDescriptor,
    EnumDescriptor,
    Descriptor,
    FileDescriptor,
)
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message

from arrowgen.generator import get_proto_module

VALID_DATA = {FieldDescriptor.TYPE_BYTES: [b"1", b"foo", b"\n"]}

VALID_CPP_DATA = {
    FieldDescriptor.CPPTYPE_INT32: [0, 1, 2, 3, 2 ** 29, -(2 ** 29)],
    FieldDescriptor.CPPTYPE_INT64: [0, 1, 2 ** 62, -6],
    FieldDescriptor.CPPTYPE_UINT32: [0, 1, 2 ** 30],
    FieldDescriptor.CPPTYPE_UINT64: [0, 1, 2, 2 ** 3],
    FieldDescriptor.CPPTYPE_DOUBLE: [
        1.0,
        2.0,
        -4,
        float("Nan"),
        float("Inf"),
        -float("Inf"),
    ],
    FieldDescriptor.CPPTYPE_FLOAT: [
        1.0,
        2.0,
        -4,
        float("Nan"),
        float("Inf"),
        -float("Inf"),
    ],
    FieldDescriptor.CPPTYPE_BOOL: [True, False],
    FieldDescriptor.CPPTYPE_STRING: ["", "FOO", "BAR", "123"],
}


def generate_message(descriptor: Descriptor, count: int) -> Message:
    message = descriptor._concrete_class()
    for one_of in descriptor.oneofs:
        one_of_index = random.randint(0, len(one_of.fields))
        if one_of_index < len(one_of.fields):
            field = one_of.fields[one_of_index]
            set_field(message, field, count)

    for field in descriptor.fields:
        if field.containing_oneof is None:
            set_field(message, field, count)
    return message


def generate_messages(descriptor: Descriptor, count: int) -> typing.List[Message]:
    return [generate_message(descriptor, count) for _ in range(count)]


def set_field(message: Message, field: FieldDescriptor, count: int) -> None:
    data = generate_field_data(field, count)
    if field.label == FieldDescriptor.LABEL_REPEATED:
        getattr(message, field.name).extend(data)
    elif field.type == FieldDescriptor.TYPE_MESSAGE:
        getattr(message, field.name).CopyFrom(data)
    else:
        setattr(message, field.name, data)


def generate_field_data(field: FieldDescriptor, count: int):
    if field.label == FieldDescriptor.LABEL_REPEATED:
        size = random.randint(0, count)
        return [_generate_data(field, count) for _ in range(size)]
    else:
        return _generate_data(field, count)


def _generate_data(field: FieldDescriptor, count):
    if field.type == FieldDescriptor.TYPE_ENUM:
        return _generate_enum(field.enum_type)
    elif field.type == FieldDescriptor.TYPE_MESSAGE:
        return generate_message(field.message_type, count)
    elif field.type in VALID_DATA:
        return random.choice(VALID_DATA[field.type])
    else:
        sample = VALID_CPP_DATA[field.cpp_type]
        return random.choice(sample)


def _generate_enum(enum: EnumDescriptor):
    return random.choice(enum.values).index


def generate_for_file(file: str, output_dir: str, count: int = 10) -> typing.List[str]:
    file_descriptor = get_proto_module(file).DESCRIPTOR
    return generate_for_file_descriptor(file_descriptor, output_dir, count)


def generate_for_file_descriptor(
    file_descriptor: FileDescriptor, output_dir: str, count: int
) -> typing.List[str]:
    results = []
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    for message in file_descriptor.message_types_by_name.values():
        data = [generate_message(message, count) for _ in range(count)]
        file_name = os.path.join(output_dir, message.name + ".jsonl")
        results.append(file_name)

        with open(file_name, "w") as fp:
            for d in data:
                fp.write(MessageToJson(d, indent=0).replace("\n", ""))
                fp.write("\n")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Generate code to convert Google Protocol Buffers to Arrow Table"
    )
    parser.add_argument("proto_file", type=str, help="Input .proto file")
    parser.add_argument("--output_dir", type=str, default="./", help="Output directory")
    parser.add_argument("--count", type=int, default=10, help="Output directory")
    args = parser.parse_args()
    files = generate_for_file(args.proto_file, args.output_dir, args.count)
    print(f"{args.proto_file} generated {' '.join(files)}")


if __name__ == "__main__":
    main()
