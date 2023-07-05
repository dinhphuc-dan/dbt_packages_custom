{% macro string_to_date(column_name,date_format='%Y%m%d') %}
    {{return(adapter.dispatch('string_to_date')(column_name, date_format))}}
{%- endmacro %}

{% macro default__string_to_date(column_name,date_format) %}
    PARSE_DATE({{date_format}}, {{column_name}})
{%- endmacro %}

{% macro bigquery__string_to_date(column_name,date_format) %}
    PARSE_DATE('{{date_format}}', {{column_name}})
{%- endmacro %}



{% macro extract_date_from_timestamp(timestamp_column)%}
     extract(date from {{timestamp_column}})
{% endmacro %}

