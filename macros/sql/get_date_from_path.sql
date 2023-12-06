{% macro get_date_from_path(column_name) %}

    date(regexp_substr({{ column_name }}, '\\d\\d\\.\\d\\d\\.\\d\\d'), 'YY.DD.MM')

{% endmacro %}
