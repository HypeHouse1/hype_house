{% macro cast_to_number(column_name) %}

    zeroifnull(try_to_number({{ column_name }}::string))::int

{% endmacro %}
