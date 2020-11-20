#include "{{file_wrapper.appender_header()}}"


{% for namespace in file_wrapper.namespaces() -%}
namespace {{namespace}} {
{% endfor %}

{% for wrapper in file_wrapper.message_wrappers() -%}

{{wrapper.appender_name()}}::{{wrapper.appender_name()}}(arrow::MemoryPool *pool)
    {% for initializer_statement in wrapper.initializer_statements() -%}
    {{ ": " if loop.first }}{{initializer_statement}}{{ "," if not loop.last }}
    {% endfor %}
{
}

arrow::Status {{wrapper.appender_name()}}::append({{wrapper.message_name()}} const& message) {
    {% for append_statement in wrapper.append_statements() -%}
    {{ append_statement }}
    {% endfor %}
    return arrow::Status::OK();

}

arrow::Status {{wrapper.appender_name()}}::build(std::shared_ptr<arrow::Table> * table) {
    {% for finish_statement in wrapper.finish_statements() -%}
    {{ finish_statement}}
    {% endfor %}

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        {% for schema_statement in wrapper.schema_statements() -%}
        {{schema_statement}}{{ "," if not loop.last }}
        {%endfor%}
    };
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(schema_vector);
    *table = arrow::Table::Make(
        schema,
        {
            {%for array in wrapper.arrays()-%}
            {{array}}{{ "," if not loop.last }}
            {% endfor %}
        }
    );

    return arrow::Status::OK();

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
    while ({{field.index_name()}} >= {{field.array_name()}}->length()) {
        {{field.index_name()}} = 0;
        ++{{field.chunk_name()}};
        {{field.array_name()}} = {{field.array_caster()}}({{field.get_array_statement()}});
        {% if field.is_repeated() %}
        {{field.pointer_name()}} = {{field.array_name()}}->values()->data()->GetValues<{{field.cpp_type()}}>(1);
        {% endif %}
    }
    {% if field.is_repeated() %}
    for (arrow::ListArray::offset_type index = {{field.array_name()}}->value_offset({{field.index_name()}});
         index < {{field.array_name()}}->value_offset({{field.index_name()}} + 1);
         ++index) {
      message.add_{{field.name()}}({{field.optional_cast()}}*({{field.pointer_name()}} + index));
    }
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
