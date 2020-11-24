import random
import subprocess

from google.protobuf.descriptor import FieldDescriptor, EnumDescriptor


def run_command(command):
    subprocess.run(command, check=True, text=True)


VALID_DATA = {
    FieldDescriptor.TYPE_BYTES: [b'1', b'foo', b'\n']
}

VALID_CPP_DATA = {
    FieldDescriptor.CPPTYPE_INT32: [0, 1, 2, 3, 2 ** 29, -2 ** 29],
    FieldDescriptor.CPPTYPE_INT64: [0, 1, 2 ** 62, -6],
    FieldDescriptor.CPPTYPE_UINT32: [0, 1, 2 ** 30],
    FieldDescriptor.CPPTYPE_UINT64: [0, 1, 2, 2 ** 3],
    FieldDescriptor.CPPTYPE_DOUBLE: [1.0, 2.0, -4, float('Nan'), float('Inf'), -float('Inf')],
    FieldDescriptor.CPPTYPE_FLOAT: [1.0, 2.0, -4, float('Nan'), float('Inf'), -float('Inf')],
    FieldDescriptor.CPPTYPE_BOOL: [True, False],
    FieldDescriptor.CPPTYPE_STRING: ["", "FOO", "BAR", "123"],
}


def generate_data(descriptor):
    message = descriptor._concrete_class()
    for field in descriptor.fields:
        data = generate_field_data(field)
        if field.label == FieldDescriptor.LABEL_REPEATED:
            getattr(message, field.name).extend(data)
        else:
            setattr(message, field.name, data)
    return message


def generate_field_data(field: FieldDescriptor):
    if field.label == FieldDescriptor.LABEL_REPEATED:
        size = random.randint(0, 10)
        return [_generate_data(field) for _ in range(size)]
    else:
        return _generate_data(field)


def _generate_data(field: FieldDescriptor):
    if field.cpp_type == FieldDescriptor.CPPTYPE_ENUM:
        return _generate_enum(field.enum_type)
    elif field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        raise ValueError("Not implemented")
    elif field.type in VALID_DATA:
        return random.choice(VALID_DATA[field.type])
    else:
        sample = VALID_CPP_DATA.get(field.cpp_type)
        return random.choice(sample)


def _generate_enum(enum: EnumDescriptor):
    return random.choice(enum.values).index
