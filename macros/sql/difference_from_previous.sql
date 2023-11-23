{% macro difference_from_previous(column_name, partition_by, order_by) %}

    {{ column_name }} - lag({{ column_name }}, 1, 0) over (
            partition by {{ partition_by }} 
            order by {{ order_by }} asc
        )

{% endmacro%}