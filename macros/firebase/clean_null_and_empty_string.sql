{% macro clean_null_and_empty_string(column_name) %}
    case when {{column_name}} is null or {{column_name}} = ''  then 'unknown'
            else {{column_name}}            
            end
{% endmacro %}