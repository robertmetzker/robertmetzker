{% macro clone_schema(schema_to_clone) -%}

    {% set all_tables_query %}
        show tables in schema {{ schema_to_clone }}
    {% endset %}

    {% set results = run_query(all_tables_query) %}

    {{ "create or replace schema " ~ generate_schema_name(var("custom_schema")) ~ ";" }}

    {% if execute %}
        {% for result_row in results %}
            {{ log("create table " ~ generate_schema_name(var("custom_schema")) ~ "." ~ result_row[1] ~ " clone " ~ schema_to_clone ~ "." ~ result_row[1] ~ ";") }}
            {{ "create table " ~ generate_schema_name(var("custom_schema")) ~ "." ~ result_row[1] ~ " clone " ~ schema_to_clone ~ "." ~ result_row[1] ~ ";" }}
        {% endfor %}
    {% endif %}
{%- endmacro %}
