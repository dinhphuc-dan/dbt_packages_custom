{% macro extract_date_from_timestamp(timestamp_value, type = 'DATE', time_zone = var('time_zone') ) %}
     extract( {{type}} from {{timestamp_value}} AT TIME ZONE '{{time_zone}}')
{% endmacro %}
