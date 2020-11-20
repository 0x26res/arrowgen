from dataclasses import dataclass

from google.protobuf.descriptor import FileDescriptor, Descriptor, FieldDescriptor

CPP_TYPES = {
    FieldDescriptor.CPPTYPE_BOOL: 'bool',
    FieldDescriptor.CPPTYPE_DOUBLE: 'double',
    FieldDescriptor.CPPTYPE_ENUM: 'int32_t',
    FieldDescriptor.CPPTYPE_FLOAT: 'float',
    FieldDescriptor.CPPTYPE_INT32: 'int32_t',
    FieldDescriptor.CPPTYPE_INT64: 'int64_t',
    # FieldDescriptor.CPPTYPE_MESSAGE: 'arrow::',
    FieldDescriptor.CPPTYPE_STRING: 'std::string',
    FieldDescriptor.CPPTYPE_UINT32: 'uint32_t',
    FieldDescriptor.CPPTYPE_UINT64: 'uint64_t',
}

CPP_BUILDERS = {
    FieldDescriptor.CPPTYPE_BOOL: 'arrow::BooleanBuilder',
    FieldDescriptor.CPPTYPE_DOUBLE: 'arrow::DoubleBuilder',
    FieldDescriptor.CPPTYPE_ENUM: 'arrow::Int32Builder',
    FieldDescriptor.CPPTYPE_FLOAT: 'arrow::FloatBuilder',
    FieldDescriptor.CPPTYPE_INT32: 'arrow::Int32Builder',
    FieldDescriptor.CPPTYPE_INT64: 'arrow::Int64Builder',
    # FieldDescriptor.CPPTYPE_MESSAGE: 'arrow::',
    FieldDescriptor.CPPTYPE_STRING: 'arrow::StringBuilder',
    FieldDescriptor.CPPTYPE_UINT32: 'arrow::UInt32Builder',
    FieldDescriptor.CPPTYPE_UINT64: 'arrow::UInt64Builder',
}

CPP_ARRAYS = {
    FieldDescriptor.CPPTYPE_BOOL: 'arrow::BooleanArray',
    FieldDescriptor.CPPTYPE_DOUBLE: 'arrow::DoubleArray',
    FieldDescriptor.CPPTYPE_ENUM: 'arrow::Int32Array',
    FieldDescriptor.CPPTYPE_FLOAT: 'arrow::FloatArray',
    FieldDescriptor.CPPTYPE_INT32: 'arrow::Int32Array',
    FieldDescriptor.CPPTYPE_INT64: 'arrow::Int64Array',
    # FieldDescriptor.CPPTYPE_MESSAGE: 'arrow::',
    FieldDescriptor.CPPTYPE_STRING: 'arrow::StringArray',
    FieldDescriptor.CPPTYPE_UINT32: 'arrow::UInt32Array',
    FieldDescriptor.CPPTYPE_UINT64: 'arrow::UInt64Array',
}

ARROW_TYPES = {
    FieldDescriptor.CPPTYPE_BOOL: 'arrow::boolean()',
    FieldDescriptor.CPPTYPE_DOUBLE: 'arrow::float64()',
    FieldDescriptor.CPPTYPE_ENUM: 'arrow::int32()',
    FieldDescriptor.CPPTYPE_FLOAT: 'arrow::float32()',
    FieldDescriptor.CPPTYPE_INT32: 'arrow::int32()',
    FieldDescriptor.CPPTYPE_INT64: 'arrow::int64()',
    # FieldDescriptor.CPPTYPE_MESSAGE: 'arrow::',
    FieldDescriptor.CPPTYPE_STRING: 'arrow::utf8()',
    FieldDescriptor.CPPTYPE_UINT32: 'arrow::uint32()',
    FieldDescriptor.CPPTYPE_UINT64: 'arrow::uint64()',
}


class ReaderField:
    def __init__(self, field: FieldDescriptor, index: int):
        self.field = field
        self.index = index

    def name(self):
        return self.field.name

    def index_name(self):
        return f'{self.field.name}_index_'

    def array_name(self):
        return f'{self.field.name}_array_'

    def chunk_name(self):
        return f'{self.field.name}_chunk_'

    def pointer_name(self):
        return f'{self.field.name}_pointer_'

    def get_pointer_statement(self):
        return f'{self.array_name()}'

    def array_caster(self):
        return f'std::static_pointer_cast<{self.array_type()}>'

    def get_array_statement(self):
        return f'table_->column({self.index})->chunk({self.chunk_name()})'

    def value_reader(self):
        if self.field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
            return 'GetString'
        else:
            return 'Value'

    def is_repeated(self):
        return self.field.label == FieldDescriptor.LABEL_REPEATED

    def is_enum(self):
        return self.field.cpp_type == FieldDescriptor.CPPTYPE_ENUM

    def optional_cast(self):
        if self.is_enum():
            return f'({self.value_type()    })'
        else:
            return ''

    def array_type(self):
        if self.is_repeated():
            return 'arrow::ListArray'
        else:
            return CPP_ARRAYS[self.field.cpp_type]

    def value_type(self):
        if self.is_enum():
            return self.field.enum_type.full_name.replace('.', '::')
        else:
            return CPP_TYPES[self.field.cpp_type]

    def cpp_type(self):
        return CPP_TYPES[self.field.cpp_type]

@dataclass
class ReaderMember:
    name: str
    cpp_type: str
    initializer: str


class ReaderStatement:
    name: str
    index: int


class MessageWrapper:

    def __init__(self, descriptor: Descriptor):
        self.descriptor = descriptor

    def header(self):
        return self.descriptor.file.name[:-5] + 'pb.h'

    def appender_name(self):
        return self.descriptor.name + 'Appender'

    def reader_name(self):
        return self.descriptor.name + 'Reader'

    def message_name(self):
        return self.descriptor.full_name.replace('.', '::')

    def builders(self):
        for field in self.descriptor.fields:
            if field.label != FieldDescriptor.LABEL_REPEATED:
                yield CPP_BUILDERS[field.cpp_type] + ' ' + field.name + '_builder_;'
            else:
                yield "arrow::ListBuilder " + field.name + '_list_builder_;'
                yield CPP_BUILDERS[field.cpp_type] + '& ' + field.name + '_builder_;'

    def initializer_statements(self):
        for field in self.descriptor.fields:
            if field.label != FieldDescriptor.LABEL_REPEATED:
                yield f'{field.name}_builder_(pool)'
            else:
                yield f'{field.name}_list_builder_(pool, std::make_shared<{CPP_BUILDERS[field.cpp_type]}>(pool))'
                yield f'{field.name}_builder_(*(static_cast<{CPP_BUILDERS[field.cpp_type]} *>({field.name}_list_builder_.value_builder())))'

    def initializer_statement(self):
        return ': ' + ',\n'.join(self.initializer_statements())

    def append_statements(self):
        for field in self.descriptor.fields:
            if field.label != FieldDescriptor.LABEL_REPEATED:
                yield f"ARROW_RETURN_NOT_OK({field.name}_builder_.Append(message.{field.name}()));"
            else:
                yield f"ARROW_RETURN_NOT_OK({field.name}_list_builder_.Append());"
                if field.cpp_type == FieldDescriptor.CPPTYPE_BOOL:
                    yield f"ARROW_RETURN_NOT_OK({field.name}_builder_.AppendValues(message.{field.name}().begin(), message.{field.name}().end()));"
                elif field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
                    yield f"for (std::string const& value : message.{field.name}()) " + '{'
                    yield f'  {field.name}_builder_.Append(value);'
                    yield '}'
                else:
                    yield f"ARROW_RETURN_NOT_OK({field.name}_builder_.AppendValues(message.{field.name}().data(), message.{field.name}().size()));"

    def finish_statements(self):
        for field in self.descriptor.fields:
            yield f'std::shared_ptr<arrow::Array> {field.name}_array;';
            if field.label != FieldDescriptor.LABEL_REPEATED:
                yield f'ARROW_RETURN_NOT_OK({field.name}_builder_.Finish(&{field.name}_array));';
            else:
                yield f'ARROW_RETURN_NOT_OK({field.name}_list_builder_.Finish(&{field.name}_array));';

    def schema_statements(self):
        for field in self.descriptor.fields:
            if field.label != FieldDescriptor.LABEL_REPEATED:
                yield f'arrow::field("{field.name}", {ARROW_TYPES[field.cpp_type]})'
            else:
                yield f'arrow::field("{field.name}", arrow::list({ARROW_TYPES[field.cpp_type]}))'

    def arrays(self):
        for field in self.descriptor.fields:
            yield f'{field.name}_array'

    def reader_members(self):
        for index, field in enumerate(self.descriptor.fields):
            yield ReaderMember(f'{field.name}_chunk_', 'uint64_t', '0')
            yield ReaderMember(f'{field.name}_index_', 'uint64_t', '0')
            array_type = (
                'arrow::ListArray'
                if field.label == FieldDescriptor.LABEL_REPEATED
                else CPP_ARRAYS[field.cpp_type]
            )
            yield ReaderMember(
                f'{field.name}_array_',
                f'std::shared_ptr<{array_type}>',
                f'std::static_pointer_cast<{array_type}> (table->column({index})->chunk(0))'
            )
            if field.label == FieldDescriptor.LABEL_REPEATED:
                yield ReaderMember(
                    f'{field.name}_pointer_',
                    f'const {CPP_TYPES.get(field.cpp_type)}*',
                    f'{field.name}_array_->values()->data()->GetValues<{CPP_TYPES[field.cpp_type]}>(1)'
                )

    def reader_fields(self):
        for index, field in enumerate(self.descriptor.fields):
            yield ReaderField(field, index)


class FileWrapper:
    def __init__(self, descriptor: FileDescriptor):
        self.descriptor = descriptor

    def message_header(self):
        return self.descriptor.name[:-5] + 'pb.h'

    def appender_header(self):
        return self.descriptor.name[:-5] + 'appender.h'

    def appender_source(self):
        return self.descriptor.name[:-5] + 'appender.cc'

    def name(self):
        return self.descriptor.name

    def namespaces(self):
        return self.descriptor.package.split('.')

    def message_wrappers(self):
        for message in self.descriptor.message_types_by_name.values():
            yield MessageWrapper(message)
