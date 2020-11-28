#pragma ONCE

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
    static arrow::FieldVector getFieldVector();

    explicit {{ wrapper.appender_name() }}(arrow::MemoryPool *pool = arrow::default_memory_pool());
    arrow::Status append({{wrapper.message_name()}} const& message);
    arrow::Status build(std::shared_ptr<arrow::Table>* table);
    arrow::Status Finish(std::shared_ptr<arrow::Array>* array);

    private:
    arrow::Status build(std::shared_ptr<arrow::StructArray>* structArray);
    arrow::Status build(arrow::ArrayVector& arrays);
    static std::vector<std::string> getFieldNames();

    {% for member in wrapper.appender_members() -%}
    {{member.cpp_type}} {{member.name}};
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

    {% for member in wrapper.reader_fields() -%}
    {% for member in member.members() -%}
    {{member.cpp_type}} {{member.name}};
    {% endfor %}
    {% endfor %}

};


{% endfor %}

{% for namespace in file_wrapper.namespaces()[::-1] -%}
} // namespace {{namespace}}
{% endfor %}