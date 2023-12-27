{% macro generate_primary_key_table() %}
    {% set fields = [] %}
        {% for i in varargs %}
        {% do fields.append("ifnull(" ~ i ~ ",'')") %}
        {% endfor %}
    {{ "REGEXP_REPLACE(CONCAT(" ~ fields | join(',') ~ "),' ','')" }}
{%- endmacro %}