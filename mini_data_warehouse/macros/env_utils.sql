{% macro generate_env_name() -%}
    {%- set default_dev = 'DEV' -%}
    {%- set default_prod = 'PROD' -%}

    {% if target.name in ['ops_dev', 'ci_dev', 'dev'] -%}
        {% set env_name = default_dev | trim | upper %}
    {% elif target.name in ['ops_prod', 'ci_prod', 'prod'] -%}
        {% set env_name = default_prod | trim | upper %}
    {% else -%}
        {% set env_name = 'DEFAULT' | trim | upper %}
    {% endif %}

    {# Log the environment name for debugging #}
    {% do log("Environment name set to: " ~ env_name, info=True) %}

    {{ env_name }}
{%- endmacro %}

{%- macro get_operations_db_name() -%}

    {%- set snowflake_env = generate_env_name() | trim -%}
    {%- set operations_db = 'OPERATIONS_' ~ snowflake_env -%}
    {% do log("operations_db set to: " ~ operations_db, info=True) %}

    {{ operations_db }}

{%- endmacro %}

