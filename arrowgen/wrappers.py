from dataclasses import dataclass
from typing import Sequence, Iterator

from google.protobuf.descriptor import FileDescriptor, Descriptor, FieldDescriptor

CPP_TYPES = {
    FieldDescriptor.TYPE_DOUBLE: "double",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT64: "int64_t",
    FieldDescriptor.TYPE_UINT64: "uint64_t",
    FieldDescriptor.TYPE_INT32: "int32_t",
    FieldDescriptor.TYPE_FIXED64: "uint64_t",
    FieldDescriptor.TYPE_FIXED32: "uint32_t",
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_STRING: "std::string",
    FieldDescriptor.TYPE_BYTES: "std::string",
    FieldDescriptor.TYPE_UINT32: "uint32_t",
    FieldDescriptor.TYPE_ENUM: "int32_t",
    FieldDescriptor.TYPE_SFIXED32: "int32_t",
    FieldDescriptor.TYPE_SFIXED64: "int64_t",
    FieldDescriptor.TYPE_SINT32: "int32_t",
    FieldDescriptor.TYPE_SINT64: "int64_t",
}

CPP_BUILDERS = {
    FieldDescriptor.TYPE_DOUBLE: "arrow::DoubleBuilder",
    FieldDescriptor.TYPE_FLOAT: "arrow::FloatBuilder",
    FieldDescriptor.TYPE_INT64: "arrow::Int64Builder",
    FieldDescriptor.TYPE_UINT64: "arrow::UInt64Builder",
    FieldDescriptor.TYPE_INT32: "arrow::Int32Builder",
    FieldDescriptor.TYPE_FIXED64: "arrow::UInt64Builder",
    FieldDescriptor.TYPE_FIXED32: "arrow::UInt32Builder",
    FieldDescriptor.TYPE_BOOL: "arrow::BooleanBuilder",
    FieldDescriptor.TYPE_STRING: "arrow::StringBuilder",
    FieldDescriptor.TYPE_BYTES: "arrow::BinaryBuilder",
    FieldDescriptor.TYPE_UINT32: "arrow::UInt32Builder",
    FieldDescriptor.TYPE_ENUM: "arrow::Int32Builder",
    FieldDescriptor.TYPE_SFIXED32: "arrow::Int32Builder",
    FieldDescriptor.TYPE_SFIXED64: "arrow::Int64Builder",
    FieldDescriptor.TYPE_SINT32: "arrow::Int32Builder",
    FieldDescriptor.TYPE_SINT64: "arrow::Int64Builder",
}

CPP_ARRAYS = {
    FieldDescriptor.TYPE_DOUBLE: "arrow::DoubleArray",
    FieldDescriptor.TYPE_FLOAT: "arrow::FloatArray",
    FieldDescriptor.TYPE_INT64: "arrow::Int64Array",
    FieldDescriptor.TYPE_UINT64: "arrow::UInt64Array",
    FieldDescriptor.TYPE_INT32: "arrow::Int32Array",
    FieldDescriptor.TYPE_FIXED64: "arrow::UInt64Array",
    FieldDescriptor.TYPE_FIXED32: "arrow::UInt32Array",
    FieldDescriptor.TYPE_BOOL: "arrow::BooleanArray",
    FieldDescriptor.TYPE_STRING: "arrow::StringArray",
    FieldDescriptor.TYPE_MESSAGE: "arrow::StructArray",
    FieldDescriptor.TYPE_BYTES: "arrow::BinaryArray",
    FieldDescriptor.TYPE_UINT32: "arrow::UInt32Array",
    FieldDescriptor.TYPE_ENUM: "arrow::Int32Array",
    FieldDescriptor.TYPE_SFIXED32: "arrow::Int32Array",
    FieldDescriptor.TYPE_SFIXED64: "arrow::Int64Array",
    FieldDescriptor.TYPE_SINT32: "arrow::Int32Array",
    FieldDescriptor.TYPE_SINT64: "arrow::Int64Array",
}

ARROW_TYPES = {
    FieldDescriptor.TYPE_DOUBLE: "arrow::float64()",
    FieldDescriptor.TYPE_FLOAT: "arrow::float32()",
    FieldDescriptor.TYPE_INT64: "arrow::int64()",
    FieldDescriptor.TYPE_UINT64: "arrow::uint64()",
    FieldDescriptor.TYPE_INT32: "arrow::int32()",
    FieldDescriptor.TYPE_FIXED64: "arrow::uint64()",
    FieldDescriptor.TYPE_FIXED32: "arrow::uint32()",
    FieldDescriptor.TYPE_BOOL: "arrow::boolean()",
    FieldDescriptor.TYPE_STRING: "arrow::utf8()",
    FieldDescriptor.TYPE_BYTES: "arrow::binary()",
    FieldDescriptor.TYPE_UINT32: "arrow::uint32()",
    FieldDescriptor.TYPE_ENUM: "arrow::int32()",
    FieldDescriptor.TYPE_SFIXED32: "arrow::int32()",
    FieldDescriptor.TYPE_SFIXED64: "arrow::int64()",
    FieldDescriptor.TYPE_SINT32: "arrow::int32()",
    FieldDescriptor.TYPE_SINT64: "arrow::int64()",
}


def shared_ptr(cpp_type: str) -> str:
    return f"std::shared_ptr<{cpp_type}>"


@dataclass
class ClassMember:
    name: str
    cpp_type: str
    initializer: str

    def to_shared_ptr(self):
        return ClassMember(
            name=self.name,
            cpp_type=shared_ptr(self.cpp_type),
            initializer=f"std::make_shared<{self.cpp_type}>({self.initializer})",
        )


class BaseField:
    def __init__(self, field: FieldDescriptor):
        self.field = field
        self.message_wrapper = (
            MessageWrapper(self.field.message_type) if self.is_message() else None
        )

    def name(self):
        return self.field.name

    def make_name(self, suffix: str):
        return f"{self.field.name}_{suffix}_"

    def is_oneof(self):
        return self.field.containing_oneof is not None

    def oneof_name(self):
        return self.field.containing_oneof.name

    def is_repeated_message(self):
        return self.is_repeated() and self.is_message()

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
            return CPP_TYPES[self.field.type]

    def appender_type(self):
        assert self.is_message()
        return self.message_wrapper.appender_name()

    def cpp_type(self):
        return CPP_TYPES[self.field.type]

    def offset_type(self):
        # TODO: implement
        return "uint64_t"

    def schema_statement(self):
        if self.is_repeated_message():
            return f'arrow::field("{self.name()}", arrow::list(arrow::struct_({self.appender_type()}::FIELD_VECTOR)))'
        elif self.is_repeated():
            return f'arrow::field("{self.name()}", arrow::list({ARROW_TYPES[self.field.type]}))'
        elif self.is_message():
            return f'arrow::field("{self.name()}", arrow::struct_({self.appender_type()}::FIELD_VECTOR))'
        else:
            return f'arrow::field("{self.name()}", {ARROW_TYPES[self.field.type]})'

    def oneof_name(self):
        assert self.is_oneof()
        return self.field.containing_oneof.name

    def has_statement(self):
        assert self.is_oneof()
        return f"has_{self.name()}()"

    def containing_class(self):
        return self.field.containing_type.full_name.replace(".", "::")


class ReaderField(BaseField):
    def __init__(self, field: FieldDescriptor):
        super().__init__(field)

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
        return f'table_->GetColumnByName("{self.name()}")->chunk({self.chunk_name()})'

    def length_statement(self):
        if (
            self.is_message() and not self.is_repeated_message()
        ):  # TODO: FIX (use shared ptr everywhere)
            return ".length()"
        else:
            return "->length()"

    def value_reader(self):
        if self.field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
            return "GetString"
        elif self.is_message():
            return
        else:
            return "Value"

    def optional_cast(self):
        if self.is_enum():
            return f"({self.value_type()})"
        else:
            return ""

    def array_type(self):
        return CPP_ARRAYS[self.field.type]

    def is_null_statement(self, index_name="index"):
        return f"{self.main_array_name()}{'.' if self.is_message() else '->'}IsNull({index_name})"

    def struct_reader_members(self) -> Sequence[ClassMember]:
        if self.is_repeated_message():
            yield ClassMember(
                self.list_array_name(),
                shared_ptr("arrow::ListArray"),
                f'{self.list_array_caster()}(struct_array_->GetFieldByName("{self.name()}"))',
            )
            yield ClassMember(
                self.array_name(),
                self.struct_reader_type(),
                f"{self.array_caster()}({self.list_array_name()}->values())",
            )

        elif self.is_repeated():
            yield ClassMember(
                self.list_array_name(),
                shared_ptr("arrow::ListArray"),
                f'{self.list_array_caster()}(struct_array_->GetFieldByName("{self.name()}"))',
            )
            yield ClassMember(
                self.array_name(),
                shared_ptr(self.array_type()),
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
                shared_ptr(CPP_ARRAYS[self.field.type]),
                f'{self.array_caster()}(struct_array_->GetFieldByName("{self.name()}"))',
            )

    def members(self) -> Sequence[ClassMember]:
        yield ClassMember(self.chunk_name(), "uint64_t", "0")
        yield ClassMember(self.index_name(), "uint64_t", "0")
        if self.is_repeated():
            yield ClassMember(
                self.list_array_name(),
                shared_ptr("arrow::ListArray"),
                f"{self.list_array_caster()}({self.get_array_statement()})",
            )
            if self.is_message():
                yield ClassMember(
                    self.array_name(),
                    self.struct_reader_type(),
                    f"{self.array_caster()}({self.list_array_name()}->values())",
                )
            else:
                yield ClassMember(
                    self.array_name(),
                    shared_ptr(self.array_type()),
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
                shared_ptr(CPP_ARRAYS[self.field.type]),
                f"{self.array_caster()}({self.get_array_statement()})",
            )

    def struct_reader_type(self):
        assert self.is_message()
        return MessageWrapper(self.field.message_type).struct_reader_name()


class AppenderField(BaseField):
    def __init__(self, field: FieldDescriptor):
        super().__init__(field)

    def appender_name(self):
        return self.make_name("appender")

    def builder_name(self):
        return self.make_name("builder")

    def array_name(self):
        return self.make_name("array")

    def list_builder_name(self):
        return self.make_name("list_builder")

    def struct_builder_name(self):
        return self.make_name("struct_builder")

    def main_builder_name(self):
        if self.is_repeated():
            return self.list_builder_name()
        elif self.is_message():
            return self.struct_builder_name()
        else:
            return self.builder_name()

    def builder_type(self):
        if self.is_message():
            return self.appender_type()
        else:
            return CPP_BUILDERS[self.field.type]

    def is_repeated(self):
        return self.field.label == FieldDescriptor.LABEL_REPEATED

    def members(self) -> Sequence[ClassMember]:
        if self.is_repeated_message():
            yield ClassMember(
                self.appender_name(), self.appender_type(), "pool"
            ).to_shared_ptr()
            yield ClassMember(
                self.struct_builder_name(),
                "arrow::StructBuilder",
                f"{self.message_wrapper.data_type_statement()}, pool, {self.appender_name()}->getBuilders()",
            ).to_shared_ptr()
            yield ClassMember(
                self.list_builder_name(),
                "arrow::ListBuilder",
                f"pool, {self.struct_builder_name()}, arrow::list({self.message_wrapper.data_type_statement()})",
            ).to_shared_ptr()
        elif self.is_repeated():
            yield ClassMember(
                self.list_builder_name(),
                "arrow::ListBuilder",
                f"pool, std::make_shared<{self.builder_type()}>(pool)",
            ).to_shared_ptr()
            yield ClassMember(
                self.builder_name(),
                self.builder_type() + "*",
                f"(static_cast < {self.builder_type()} * > ({self.list_builder_name()}->value_builder()))",
            )
        elif self.is_message():
            yield ClassMember(
                self.appender_name(), self.appender_type(), "pool"
            ).to_shared_ptr()
            yield ClassMember(
                self.struct_builder_name(),
                "arrow::StructBuilder",
                f"{self.message_wrapper.data_type_statement()}, pool, {self.appender_name()}->getBuilders()",
            ).to_shared_ptr()
        else:
            yield ClassMember(
                self.builder_name(), self.builder_type(), "pool"
            ).to_shared_ptr()

    def append_statements(self):
        if self.is_oneof():
            yield f"if (message.{self.has_statement()})"
            yield "{"
            for s in self.exists_append_statements():
                yield s
            yield "}"
            yield "else"
            yield "{"
            for s in self.missing_append_statements():
                yield s
            yield "}"
        else:
            for s in self.exists_append_statements():
                yield s

    def missing_append_statements(self) -> Iterator[str]:
        if self.is_repeated():
            yield f"ARROW_RETURN_NOT_OK({self.list_builder_name()}->AppendNull());"
        elif self.is_message():
            yield f"ARROW_RETURN_NOT_OK({self.struct_builder_name()}->AppendNull());"
        else:
            yield f"ARROW_RETURN_NOT_OK({self.builder_name()}->AppendNull());"

    def exists_append_statements(self) -> Iterator[str]:
        """TODO: Use jinja for this"""
        if self.is_repeated_message():
            yield f"ARROW_RETURN_NOT_OK({self.list_builder_name()}->Append());"
            yield f"for (const {self.message_wrapper.message_name()}& value : message.{self.name()}())"
            yield "{"
            yield f"  {self.struct_builder_name()}->Append();"
            yield f"  {self.appender_name()}->append(value);"
            yield "}"
        elif self.is_repeated():
            yield f"ARROW_RETURN_NOT_OK({self.list_builder_name()}->Append());"
            if self.is_boolean():
                yield f"ARROW_RETURN_NOT_OK({self.builder_name()}->AppendValues(message.{self.name()}().begin(), message.{self.name()}().end()));"
            elif self.is_string():
                yield f"for (std::string const& value : message.{self.name()}()) " + "{"
                yield f"  {self.builder_name()}->Append(value);"
                yield "}"
            else:
                yield f"ARROW_RETURN_NOT_OK({self.builder_name()}->AppendValues(message.{self.name()}().data(), message.{self.name()}().size()));"
        elif self.is_message():
            yield f"ARROW_RETURN_NOT_OK({self.struct_builder_name()}->Append());"
            yield f"ARROW_RETURN_NOT_OK({self.appender_name()}->append(message.{self.name()}()));"
        else:
            yield f"ARROW_RETURN_NOT_OK({self.builder_name()}->Append(message.{self.name()}()));"

    def finish_statements(self):
        yield f"std::shared_ptr<arrow::Array> {self.array_name()};"
        if self.is_repeated():
            yield f"ARROW_RETURN_NOT_OK({self.list_builder_name()}->Finish(&{self.array_name()}));"
        elif self.is_message():
            yield f"ARROW_RETURN_NOT_OK({self.struct_builder_name()}->Finish(&{self.array_name()}));"
        else:
            yield f"ARROW_RETURN_NOT_OK({self.builder_name()}->Finish(&{self.array_name()}));"
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
        for field in self.descriptor.fields:
            yield ReaderField(field)

    def appender_fields(self) -> Sequence[AppenderField]:
        for field in self.descriptor.fields:
            yield AppenderField(field)

    def appender_members(self) -> Sequence[ClassMember]:
        for appender_field in self.appender_fields():
            for member in appender_field.members():
                yield member

    def field_names(self):
        for field in self.descriptor.fields:
            yield field.name

    def data_type_statement(self):
        return f"{self.appender_name()}::DATA_TYPE"


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
