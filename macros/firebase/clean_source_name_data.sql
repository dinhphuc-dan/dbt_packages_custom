{% macro clean_source_name_data(column_name) %}
    case when {{column_name}} is null or {{column_name}} in ('','(direct)') then 'unknown'
            else {{column_name}}            
            end
{% endmacro %}