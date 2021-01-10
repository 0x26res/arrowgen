#include "{{file_wrapper.appender_header()}}"


{% for namespace in file_wrapper.namespaces() -%}
namespace {{namespace}} {
{% endfor %}

{% for wrapper in file_wrapper.message_wrappers() -%}

{{wrapper.appender_name()}}::{{wrapper.appender_name()}}(arrow::MemoryPool *pool)
    {% for member in wrapper.appender_members() -%}
    {{ ": " if loop.first }}{{member.name}}({{member.initializer}}){{ "," if not loop.last }}
    {% endfor %}
{
}

arrow::Status {{wrapper.appender_name()}}::append(const char* bytes, size_t size) {
  {{wrapper.message_name()}} message;
  if (!message.ParseFromArray(bytes, size)) {
    return arrow::Status::SerializationError("Could not serialize {{wrapper.message_name()}}");
  } else {
    return this->append(message);
  }
}

arrow::Status {{wrapper.appender_name()}}::append({{wrapper.message_name()}} const& message) {
    {% for append_statement in wrapper.append_statements() -%}
    {{ append_statement }}
    {% endfor %}
    return arrow::Status::OK();

}

arrow::Status {{wrapper.appender_name()}}::build(arrow::ArrayVector& arrays) {
    {% for finish_statement in wrapper.finish_statements() -%}
    {{ finish_statement }}
    {% endfor %}
    return arrow::Status::OK();
}


const arrow::FieldVector {{wrapper.appender_name()}}::FIELD_VECTOR = {
    {% for schema_statement in wrapper.schema_statements() -%}
    {{schema_statement}}{{ "," if not loop.last }}
    {% endfor %}
};


//std::vector<std::shared_ptr<arrow::ArrayBuilder>> {{wrapper.appender_name()}}::getBuilders() {
//  return {
//    {% for field in wrapper.appender_fields() -%}
//      std::static_pointer_cast<arrow::StringArray>(this->{{field.builder_name()}}),
//    {% endfor %}
//  };
//}

arrow::Status {{wrapper.appender_name()}}::build(std::shared_ptr<arrow::Table> * table) {
    arrow::ArrayVector arrays;
    ARROW_RETURN_NOT_OK(this->build(arrays));
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>({{wrapper.appender_name()}}::FIELD_VECTOR);
    *table = arrow::Table::Make(
        schema,
        arrays
    );
    return arrow::Status::OK();
}

arrow::Status {{wrapper.appender_name()}}::Finish(std::shared_ptr<arrow::Array>* array) {
    std::shared_ptr<arrow::StructArray> struct_array;
    ARROW_RETURN_NOT_OK(this->build(&struct_array));
    *array = struct_array;
    return arrow::Status::OK();
}

const std::vector<std::string> {{wrapper.appender_name()}}::FIELD_NAMES = {
    {% for field_name in wrapper.field_names() -%}
    "{{ field_name }}"{{ "," if not loop.last }}
    {% endfor %}
};


arrow::Status {{wrapper.appender_name()}}::build(std::shared_ptr<arrow::StructArray> * struct_array) {
    arrow::ArrayVector arrays;
    ARROW_RETURN_NOT_OK(this->build(arrays));
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>({{wrapper.appender_name()}}::FIELD_VECTOR);

    arrow::Result<std::shared_ptr<arrow::StructArray>> result = arrow::StructArray::Make(arrays, {{wrapper.appender_name()}}::FIELD_NAMES);
    ARROW_RETURN_NOT_OK(result.status());
    *struct_array = result.ValueUnsafe();
    return arrow::Status::OK();
}


{{wrapper.struct_reader_name()}}::{{wrapper.struct_reader_name()}}(std::shared_ptr<arrow::StructArray> struct_array)
    : struct_array_(struct_array),
    {% for array_member in wrapper.struct_reader_members() -%}
    {{array_member.name}}({{array_member.initializer}}){{ "," if not loop.last }}
    {% endfor %}
{
}


arrow::Status {{wrapper.struct_reader_name()}}::GetValue(uint64_t const index, {{wrapper.message_name()}} &message) const {
  if (index >= struct_array_->length()) {
    return arrow::Status::IndexError("Too Far");
  } else {
    {% for field in wrapper.reader_fields() -%}
    {% if field.is_repeated() -%}
    for ({{field.offset_type()}} value_index = {{field.list_array_name()}}->value_offset(index);
         value_index < {{field.list_array_name()}}->value_offset(index + 1);
         ++value_index) {
      message.add_{{field.name()}}({{field.optional_cast()}}{{field.array_name()}}->{{field.value_reader()}}(value_index) );
    }
    {% else %}
    {% if field.is_message() %}
    {{field.value_type()}}* {{field.name()}} = new {{field.value_type()}}();
    {{field.array_name()}}.GetValue(index, *{{field.name()}});
    message.set_allocated_{{field.name()}}({{field.name()}});
    {% else %}
    message.set_{{field.name()}}({{field.optional_cast()}}{{field.array_name()}}->{{field.value_reader()}}(index));
    {% endif %}
    {% endif %}
    {% endfor %}
    return arrow::Status::OK();
  }
}

uint64_t {{wrapper.struct_reader_name()}}::length() const {
    return struct_array_->length();
}

{{wrapper.reader_name()}}::{{wrapper.reader_name()}}(std::shared_ptr<arrow::Table> table)
: table_(table),
current_(0),
    {% for reader_member in wrapper.reader_members() -%}
    {{reader_member.name}}({{reader_member.initializer}}){{ "," if not loop.last }}
    {% endfor %}
{
}

bool {{wrapper.reader_name()}}::end() const {
  return current_ >= table_->num_rows();
}


arrow::Status {{wrapper.reader_name()}}::readNext({{wrapper.message_name()}} &message) {
  if (end()) {
    return arrow::Status::IndexError("Too far");
  } else {
    {% for field in wrapper.reader_fields() -%}
    while ({{field.index_name()}} >= {{field.main_array_name()}}{{field.length_statement()}}) {
        {{field.index_name()}} = 0;
        ++{{field.chunk_name()}};
        {% if field.is_repeated() %}
        {{field.list_array_name()}} = {{field.list_array_caster()}}({{field.get_array_statement()}});
        {{field.array_name()}} = {{field.array_caster()}}({{field.list_array_name()}}->values());
        {% else %}
        {{field.array_name()}} = {{field.array_caster()}}({{field.get_array_statement()}});
        {% endif %}
    }
    {% if field.is_repeated() %}
    for ({{field.offset_type()}} index = {{field.list_array_name()}}->value_offset({{field.index_name()}});
         index < {{field.list_array_name()}}->value_offset({{field.index_name()}} + 1);
         ++index) {
      message.add_{{field.name()}}({{field.optional_cast()}}{{field.array_name()}}->{{field.value_reader()}}(index) );
    }
    {% elif field.is_message() %}
    {{field.value_type()}}* {{field.name()}} = new {{field.value_type()}}();
    {{field.array_name()}}.GetValue({{field.index_name()}}, *{{field.name()}});
    message.set_allocated_{{field.name()}}({{field.name()}});
    {% else %}
    message.set_{{field.name()}}({{field.optional_cast()}}{{field.array_name()}}->{{field.value_reader()}}({{field.index_name()}}));
    {% endif %}
    ++{{field.index_name()}};
    {% endfor %}

    ++current_;
    return arrow::Status::OK();
  }

}

{% endfor %}


{% for namespace in file_wrapper.namespaces()[::-1] -%}
} // namespace {{namespace}}
{% endfor %}
