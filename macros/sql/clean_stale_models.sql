{% macro clean_stale_models(days=7, clean=false) %}

    {% set table_catalog = 'TABLE_CATALOG' %}
    {% set table_schema = 'TABLE_SCHEMA' %}
    {% set table_name = 'TABLE_NAME' %}
    {% set table_type = 'TABLE_TYPE' %}

    {% set base_table = 'BASE TABLE' %}
    {% set temp_table = 'TEMPORARY TABLE' %}
    {% set view = 'VIEW' %}
    {% set mat_view = 'MATERIALIZED VIEW' %}

    {% set query %}

        use role {{ target.role }};
        use warehouse {{ target.warehouse }};
        select 
            {{ table_catalog }}
            , {{ table_schema }}
            , {{ table_name }}
            , {{ table_type }}
        from {{ target.database }}.information_schema.tables
        where 1 = 1
            and timestampdiff(day, last_altered, localtimestamp()) > {{ days }}
            and timestampdiff(day, last_ddl, localtimestamp()) > {{ days }}
            and table_owner = '{{ target.role | upper }}'
            and {{ table_type }} in ('{{ base_table }}', '{{ temp_table }}', '{{ view }}', '{{ mat_view }}');

    {% endset %}

    {% set result = run_query(query) %}

    {% if execute %}
        {% set rows = result.rows %}

        {% if rows | length == 0 %}

            {{ log('There are no stale models.') }}

        {% else %}

            {{ log('Found the following stale models:') }}

            {% for row in rows %}
            
                {{ log('\t' + row[table_type] + ' ' + row[table_catalog] + '.' + row[table_schema] + '.' + row[table_name]) }}

                {% if clean %}

                    {% if row[table_type] == view %}

                        {% set object_type = 'view' %}

                    {% elif row[table_type] == mat_view %}

                        {% set object_type = 'materialized view' %}

                    {% else %}

                        {% set object_type = 'table' %}

                    {% endif %}

                    {% set query %}

                        drop {{ object_type }} {{ row[table_catalog] }}.{{ row[table_schema] }}.{{ row[table_name] }};

                    {% endset %}

                    {{ run_query(query) }}

                    {{ log('Dropped ' + row[table_type] + ' ' + row[table_catalog] + '.' + row[table_schema] + '.' + row[table_name])}}
                
                {% endif %}
            
            {% endfor %}
        
        {% endif %}

    {% endif %}

{% endmacro %}