{% macro classify_source_of_users(column_name) %}
    case when {{column_name}} = 'cpc' then 'paid_users'
            else 'organic_and_others'
            end
{% endmacro %}