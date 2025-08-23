{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {% if target.name == 'prod' %}
        {{ generate_custom_database_name_for_prod(custom_database_name, node) }}
    {% elif target.name == 'dev' %}
        {{ generate_custom_database_name_for_dev(custom_database_name, node) }}
    {% elif target.name == 'ci_dev' or target.name == 'ops_dev' %}
        {{ generate_custom_database_name_for_ci_dev(custom_database_name, node) }}
    {% elif target.name == 'ci_prod' or target.name == 'ops_prod' %}
        {{ generate_custom_database_name_for_ci_prod(custom_database_name, node) }}
    {% else %}
        {{ custom_database_name | trim | upper }}  {# Default case or handle as needed #}
    {% endif %}

{%- endmacro %}


{% macro generate_custom_database_name_for_dev(custom_database_name, node) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {{ default_database | trim | upper }}

    {%- else -%}

        {{ custom_database_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}

{% macro generate_custom_database_name_for_prod(custom_database_name, node) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {{ default_database | trim | upper }}

    {%- else -%}

        {{ custom_database_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}


{% macro generate_custom_database_name_for_ci_dev(custom_database_name, node) -%}

    {%- set ci_dev_default_database = "OPERATIONS_DEV" -%}
    {%- if custom_database_name is none -%}

        {{ ci_dev_default_database | trim | upper }}

    {%- else -%}

        {{ custom_database_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}

{% macro generate_custom_database_name_for_ci_prod(custom_database_name, node) -%}

    {%- set ci_prod_default_database = "OPERATIONS_PROD" -%}
    {%- if custom_database_name is none -%}

        {{ ci_prod_default_database | trim | upper }}

    {%- else -%}

        {{ custom_database_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}