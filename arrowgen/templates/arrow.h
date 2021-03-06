#ifndef {{ file_wrapper.include_guard_name() }}
#define {{ file_wrapper.include_guard_name() }}

// Source: {{file_wrapper.name() }}
// Generated code, do not modify

#include <arrow/api.h>
#include "{{file_wrapper.message_header()}}"

{% for namespace in file_wrapper.namespaces() -%}
namespace {{namespace}} {
{% endfor %}

{% for wrapper in file_wrapper.message_wrappers() -%}
class {{ wrapper.appender_name() }} {
    public:
    static const arrow::FieldVector FIELD_VECTOR;
    static const std::vector<std::string> FIELD_NAMES;
    static const std::shared_ptr<arrow::DataType> DATA_TYPE;
    static const std::shared_ptr<arrow::Schema> SCHEMA;

    explicit {{ wrapper.appender_name() }}(arrow::MemoryPool *pool = arrow::default_memory_pool());
    arrow::Status append(const char* bytes, size_t size);
    arrow::Status append({{wrapper.message_name()}} const& message);
    arrow::Status build(std::shared_ptr<arrow::Table>* table);
    arrow::Status Finish(std::shared_ptr<arrow::Array>* array);

    std::vector<std::shared_ptr<arrow::ArrayBuilder>> getBuilders();

    private:
    arrow::Status build(std::shared_ptr<arrow::StructArray>* struct_array);
    arrow::Status build(arrow::ArrayVector& arrays);

    {% for member in wrapper.appender_members() -%}
    {{member.cpp_type}} {{member.name}};
    {% endfor %}
};

class {{wrapper.struct_reader_name() }} {
  public:
    {{ wrapper.struct_reader_name() }}(std::shared_ptr<arrow::StructArray> struct_array);
    bool IsNull(uint64_t const index) const;
    arrow::Status GetValue(uint64_t const index, {{wrapper.message_name()}}& message) const;
    uint64_t length() const;

  private:
    std::shared_ptr<arrow::StructArray> struct_array_;
    {% for reader_field in wrapper.reader_fields() -%}
    {% for member in reader_field.struct_reader_members() -%}
    {{member.cpp_type}} {{member.name}};
    {% endfor %}
    {% endfor %}

};

class {{ wrapper.reader_name()}} {
    public:
    {{ wrapper.reader_name() }}(std::shared_ptr<arrow::Table> table);
    arrow::Status readNext({{wrapper.message_name()}}& message);
    bool end() const;

    private:
    std::shared_ptr<arrow::Table> table_;
    uint64_t current_;

    {% for reader_field in wrapper.reader_fields() -%}
    {% for member in reader_field.members() -%}
    {{member.cpp_type}} {{member.name}};
    {% endfor %}
    {% endfor %}

};


{% endfor %}

{% for namespace in file_wrapper.namespaces()[::-1] -%}
} // namespace {{namespace}}
{% endfor %}

#endif