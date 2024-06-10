{% macro drop_view(relation) %}
    {% set sql %}
        DROP VIEW IF EXISTS {{ relation }};
    {% endset %}
    {{ run_query(sql) }}
{% endmacro %}
