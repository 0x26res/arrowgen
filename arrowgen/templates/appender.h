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
    explicit {{ wrapper.appender_name() }}(arrow::MemoryPool *pool = arrow::default_memory_pool());
    arrow::Status append({{wrapper.message_name()}} const& message);
    arrow::Status build(std::shared_ptr<arrow::Table> * table);

    private:
    {% for builder in wrapper.builders() -%}
    {{builder}}
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

    {% for reader_member in wrapper.reader_members() -%}
    {{reader_member.cpp_type}} {{reader_member.name}};
    {% endfor %}

};


{% endfor %}

{% for namespace in file_wrapper.namespaces()[::-1] -%}
} // namespace {{namespace}}
{% endfor %}