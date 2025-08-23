{% macro generate_schema_name_for_ci(node) -%}

    {%- set ci_default_schema = env_var('CI_SCHEMA', 'UNKNOWN_PR') -%}

    {%- if target.name == 'ci_dev' or target.name == 'ci_prod' -%}
        {# Always use ci_default_schema for ci environments #}
        "{{ ci_default_schema | trim  | upper }}"
    {%- else -%}
        {% do exceptions.raise("custom_schema_name is required for non-ci environments.") %}
    {%- endif %}

{%- endmacro %}

{% macro generate_schema_name_for_ops(node) -%}

    {%- if target.name == 'ops_dev' or target.name == 'ops_prod' -%}
        {# Always use target.schema for ops environments #}
        {{ target.schema | upper | trim }}
    {%- else -%}
        {% do exceptions.raise("custom_schema_name is required for non-ops environments.") %}
    {%- endif %}

{%- endmacro %}


{% macro generate_schema_name(custom_schema_name, node) -%}

    {% if target.name == 'prod' or target.name == 'dev' %}
        {# Use custom_schema_name for main environments #}

        {{ custom_schema_name | trim | upper }}

    {% elif target.name == 'ops_dev' or target.name == 'ops_prod' %}
        {# Always use target.schema for ops environments #}

        {{ generate_schema_name_for_ops(node) }}

    {% elif target.name == 'ci_dev' or target.name == 'ci_prod' -%}
        {# Use logic for CI environments if applicable #}

        {{ generate_schema_name_for_ci(node) }}
    {% endif %}

{%- endmacro %}