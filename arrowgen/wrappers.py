from dataclasses import dataclass
from typing import Sequence

from google.protobuf.descriptor import FileDescriptor, Descriptor, FieldDescriptor

CPP_TYPES = {
    FieldDescriptor.CPPTYPE_BOOL: "bool",
    FieldDescriptor.CPPTYPE_DOUBLE: "double",
    FieldDescriptor.CPPTYPE_ENUM: "int32_t",
    FieldDescriptor.CPPTYPE_FLOAT: "float",
    FieldDescriptor.CPPTYPE_INT32: "int32_t",
    FieldDescriptor.CPPTYPE_INT64: "int64_t",
    FieldDescriptor.CPPTYPE_STRING: "std::string",
    FieldDescriptor.CPPTYPE_UINT32: "uint32_t",
    FieldDescriptor.CPPTYPE_UINT64: "uint64_t",
}

CPP_BUILDERS = {
    FieldDescriptor.CPPTYPE_BOOL: "arrow::BooleanBuilder",
    FieldDescriptor.CPPTYPE_DOUBLE: "arrow::DoubleBuilder",
    FieldDescriptor.CPPTYPE_ENUM: "arrow::Int32Builder",
    FieldDescriptor.CPPTYPE_FLOAT: "arrow::FloatBuilder",
    FieldDescriptor.CPPTYPE_INT32: "arrow::Int32Builder",
    FieldDescriptor.CPPTYPE_INT64: "arrow::Int64Builder",
    FieldDescriptor.CPPTYPE_STRING: "arrow::StringBuilder",
    FieldDescriptor.CPPTYPE_UINT32: "arrow::UInt32Builder",
    FieldDescriptor.CPPTYPE_UINT64: "arrow::UInt64Builder",
}

CPP_ARRAYS = {
    FieldDescriptor.CPPTYPE_BOOL: "arrow::BooleanArray",
    FieldDescriptor.CPPTYPE_DOUBLE: "arrow::DoubleArray",
    FieldDescriptor.CPPTYPE_ENUM: "arrow::Int32Array",
    FieldDescriptor.CPPTYPE_FLOAT: "arrow::FloatArray",
    FieldDescriptor.CPPTYPE_INT32: "arrow::Int32Array",
    FieldDescriptor.CPPTYPE_INT64: "arrow::Int64Array",
    FieldDescriptor.CPPTYPE_MESSAGE: "arrow::StructArray",
    FieldDescriptor.CPPTYPE_STRING: "arrow::StringArray",
    FieldDescriptor.CPPTYPE_UINT32: "arrow::UInt32Array",
    FieldDescriptor.CPPTYPE_UINT64: "arrow::UInt64Array",
}

ARROW_TYPES = {
    FieldDescriptor.CPPTYPE_BOOL: "arrow::boolean()",
    FieldDescriptor.CPPTYPE_DOUBLE: "arrow::float64()",
    FieldDescriptor.CPPTYPE_ENUM: "arrow::int32()",
    FieldDescriptor.CPPTYPE_FLOAT: "arrow::float32()",
    FieldDescriptor.CPPTYPE_INT32: "arrow::int32()",
    FieldDescriptor.CPPTYPE_INT64: "arrow::int64()",
    FieldDescriptor.CPPTYPE_STRING: "arrow::utf8()",
    FieldDescriptor.CPPTYPE_UINT32: "arrow::uint32()",
    FieldDescriptor.CPPTYPE_UINT64: "arrow::uint64()",
}


@dataclass
class ClassMember:
    name: str
    cpp_type: str
    initializer: str


class BaseField:
    def __init__(self, field: FieldDescriptor, index: int):
        self.field = field
        self.index = index

    def name(self):
        return self.field.name

    def make_name(self, suffix: str):
        return self.field.name + "_" + suffix + "_"

    def is_repeated(self):
        return self.field.label == FieldDescriptor.LABEL_REPEATED

    def is_enum(self):
        return self.field.cpp_type == FieldDescriptor.CPPTYPE_ENUM

    def is_boolean(self):
        return self.field.cpp_type == FieldDescriptor.CPPTYPE_BOOL

    def is_string(self):
        return self.field.cpp_type == FieldDescriptor.CPPTYPE_STRING

    def is_message(self):
        return self.field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE

    def value_type(self):
        if self.is_enum():
            return self.field.enum_type.full_name.replace(".", "::")
        elif self.is_message():
            return self.field.message_type.full_name.replace(".", "::")
        else:
            return CPP_TYPES[self.field.cpp_type]

    def appender_type(self):
        assert self.is_message()
        return MessageWrapper(self.field.message_type).appender_name()

    def cpp_type(self):
        return CPP_TYPES[self.field.cpp_type]

    def offset_type(self):
        # TODO: implement
        return "uint64_t"

    def schema_statement(self):
        if self.is_repeated():
            return f'arrow::field("{self.name()}", arrow::list({ARROW_TYPES[self.field.cpp_type]}))'
        elif self.is_message():
            return f'arrow::field("{self.name()}", arrow::struct_({self.appender_type()}::getFieldVector()))'
        else:
            return f'arrow::field("{self.name()}", {ARROW_TYPES[self.field.cpp_type]})'


class ReaderField(BaseField):
    def __init__(self, field: FieldDescriptor, index: int):
        super().__init__(field, index)

    def index_name(self):
        return self.make_name("index")

    def array_name(self):
        return self.make_name("array")

    def list_array_name(self):
        return self.make_name("list_array")

    def main_array_name(self):
        if self.is_repeated():
            return self.list_array_name()
        else:
            return self.array_name()

    def chunk_name(self):
        return self.make_name("chunk")

    def list_array_caster(self):
        return "std::static_pointer_cast<arrow::ListArray>"

    def array_caster(self):
        return f"std::static_pointer_cast<{self.array_type()}>"

    def get_array_statement(self):
        return f"table_->column({self.index})->chunk({self.chunk_name()})"

    def length_statement(self):
        if self.is_message():
            return ".length()"
        else:
            return "->length()"

    def value_reader(self):
        if self.field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
            return "GetString"
        else:
            return "Value"

    def optional_cast(self):
        if self.is_enum():
            return f"({self.value_type()})"
        else:
            return ""

    def array_type(self):
        return CPP_ARRAYS[self.field.cpp_type]

    def struct_reader_members(self) -> Sequence[ClassMember]:
        if self.is_repeated():
            yield ClassMember(
                self.list_array_name(),
                f"std::shared_ptr<arrow::ListArray>",
                f'{self.list_array_caster()}(struct_array_->GetFieldByName("{self.name()}"))',
            )
            yield ClassMember(
                self.array_name(),
                f"std::shared_ptr<{self.array_type()}>",
                f"{self.array_caster()}({self.list_array_name()}->values())",
            )
        elif self.is_message():
            yield ClassMember(
                self.array_name(),
                self.struct_reader_type(),
                f'{self.array_caster()}(struct_array_->GetFieldByName("{self.name()}"))',
            )
        else:
            yield ClassMember(
                self.array_name(),
                f"std::shared_ptr<{CPP_ARRAYS[self.field.cpp_type]}>",
                f'{self.array_caster()}(struct_array_->GetFieldByName("{self.name()}"))',
            )

    def members(self) -> Sequence[ClassMember]:
        yield ClassMember(self.chunk_name(), "uint64_t", "0")
        yield ClassMember(self.index_name(), "uint64_t", "0")
        if self.is_repeated():
            yield ClassMember(
                self.list_array_name(),
                f"std::shared_ptr<arrow::ListArray>",
                f"{self.list_array_caster()}({self.get_array_statement()})",
            )
            yield ClassMember(
                self.array_name(),
                f"std::shared_ptr<{self.array_type()}>",
                f"{self.array_caster()}({self.list_array_name()}->values())",
            )
        elif self.is_message():
            yield ClassMember(
                self.array_name(),
                self.struct_reader_type(),
                f"{self.array_caster()}({self.get_array_statement()})",
            )
        else:
            yield ClassMember(
                self.array_name(),
                f"std::shared_ptr<{CPP_ARRAYS[self.field.cpp_type]}>",
                f"{self.array_caster()}({self.get_array_statement()})",
            )

    def struct_reader_type(self):
        assert self.is_message()
        return MessageWrapper(self.field.message_type).struct_reader_name()


class AppenderField(BaseField):
    def __init__(self, field: FieldDescriptor, index: int):
        super().__init__(field, index)

    def builder_name(self):
        return self.field.name + "_builder_"

    def array_name(self):
        return self.field.name + "_array"

    def list_builder_name(self):
        return self.field.name + "_list_builder_"

    def builder_type(self):
        if self.is_message():
            return self.appender_type()
        else:
            return CPP_BUILDERS[self.field.cpp_type]

    def is_repeated(self):
        return self.field.label == FieldDescriptor.LABEL_REPEATED

    def members(self) -> Sequence[ClassMember]:
        if self.is_repeated():
            yield ClassMember(
                self.list_builder_name(),
                "arrow::ListBuilder",
                f"pool, std::make_shared<{self.builder_type()}>(pool)",
            )
            yield ClassMember(
                self.builder_name(),
                self.builder_type() + "&",
                f"*(static_cast < {self.builder_type()} * > ({self.list_builder_name()}.value_builder()))",
            )
        else:
            yield ClassMember(self.builder_name(), self.builder_type(), "pool")

    def append_statements(self):
        if self.is_repeated():
            yield f"ARROW_RETURN_NOT_OK({self.list_builder_name()}.Append());"
            if self.is_boolean():
                yield f"ARROW_RETURN_NOT_OK({self.builder_name()}.AppendValues(message.{self.name()}().begin(), message.{self.name()}().end()));"
            elif self.is_string():
                yield f"for (std::string const& value : message.{self.name()}()) " + "{"
                yield f"  {self.builder_name()}.Append(value);"
                yield "}"
            else:
                yield f"ARROW_RETURN_NOT_OK({self.builder_name()}.AppendValues(message.{self.name()}().data(), message.{self.name()}().size()));"
        elif self.is_message():
            yield f"ARROW_RETURN_NOT_OK({self.builder_name()}.append(message.{self.name()}()));"
        else:
            yield f"ARROW_RETURN_NOT_OK({self.builder_name()}.Append(message.{self.name()}()));"

    def finish_statements(self):
        yield f"std::shared_ptr<arrow::Array> {self.array_name()};"
        if self.is_repeated():
            yield f"ARROW_RETURN_NOT_OK({self.list_builder_name()}.Finish(&{self.array_name()}));"
        else:
            yield f"ARROW_RETURN_NOT_OK({self.builder_name()}.Finish(&{self.array_name()}));"
        yield f"arrays.push_back({self.array_name()});"


class MessageWrapper:
    def __init__(self, descriptor: Descriptor):
        self.descriptor = descriptor

    def header(self):
        return self.descriptor.file.name[:-5] + "pb.h"

    def appender_name(self):
        return self.descriptor.name + "Appender"

    def reader_name(self):
        # TODO: Rename to TableReader
        return self.descriptor.name + "Reader"

    def struct_reader_name(self):
        return self.descriptor.name + "StructReader"

    def message_name(self):
        return self.descriptor.full_name.replace(".", "::")

    def append_statements(self):
        for field in self.appender_fields():
            for append_statement in field.append_statements():
                yield append_statement

    def finish_statements(self):
        for appender_field in self.appender_fields():
            for finish_statement in appender_field.finish_statements():
                yield finish_statement

    def schema_statements(self):
        for field in self.appender_fields():
            yield field.schema_statement()

    def arrays(self):
        for field in self.descriptor.fields:
            yield f"{field.name}_array"

    def reader_members(self) -> Sequence[ClassMember]:
        for reader_field in self.reader_fields():
            for reader_member in reader_field.members():
                yield reader_member

    def struct_reader_members(self) -> Sequence[ClassMember]:
        for reader_field in self.reader_fields():
            for array_member in reader_field.struct_reader_members():
                yield array_member

    def reader_fields(self) -> Sequence[ReaderField]:
        for index, field in enumerate(self.descriptor.fields):
            yield ReaderField(field, index)

    def appender_fields(self) -> Sequence[AppenderField]:
        for index, field in enumerate(self.descriptor.fields):
            yield AppenderField(field, index)

    def appender_members(self) -> Sequence[ClassMember]:
        for appender_field in self.appender_fields():
            for member in appender_field.members():
                yield member

    def field_names(self):
        for field in self.descriptor.fields:
            yield field.name


class FileWrapper:
    def __init__(self, descriptor: FileDescriptor):
        self.descriptor = descriptor

    def message_header(self):
        return self.base_name() + ".pb.h"

    def appender_header(self):
        return self.base_name() + ".arrow.h"

    def appender_source(self):
        return self.base_name() + ".arrow.cc"

    def base_name(self) -> str:
        return self.descriptor.name[:-6]

    def name(self):
        return self.descriptor.name

    def namespaces(self):
        return self.descriptor.package.split(".")

    def message_wrappers(self):
        for message in self.descriptor.message_types_by_name.values():
            yield MessageWrapper(message)

    def include_guard_name(self):
        return (
            self.descriptor.package.replace(".", "_").upper()
            + "_"
            + self.base_name().upper()
        )
