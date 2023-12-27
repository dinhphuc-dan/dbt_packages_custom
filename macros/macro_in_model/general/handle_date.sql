{% macro string_to_date(column_name,date_format='%Y%m%d') %}
    {{return(adapter.dispatch('string_to_date')(column_name, date_format))}}
{%- endmacro %}

{% macro default__string_to_date(column_name,date_format) %}
    PARSE_DATE({{date_format}}, {{column_name}})
{%- endmacro %}

{% macro bigquery__string_to_date(column_name,date_format) %}
    PARSE_DATE('{{date_format}}', {{column_name}})
{%- endmacro %}

{% macro today(string_format = '%Y%m%d' ,time_zone = 'Asia/Ho_Chi_Minh')%}
     format_date('{{string_format}}',current_date('{{time_zone}}'))
{% endmacro %}