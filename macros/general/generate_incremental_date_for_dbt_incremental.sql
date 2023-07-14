{% macro generate_incremental_date_for_dbt_incremental(number_day_backwards = 7) %}
    {% set list_date_backwards = [] %}
    {% if number_day_backwards == 0 %}
        {{return('current_date()')}}
    {% elif number_day_backwards > 0 and number_day_backwards < 60 %}
        {% set number_day_backwards = number_day_backwards + 1 %}
        {% for number in range(1, number_day_backwards, 1) %}
            {% do list_date_backwards.append('date_sub(current_date(), interval '~number~' day)') %}
        {% endfor %}
        {{ list_date_backwards | join(',') }}
    {%else%}
        None
    {% endif %}
{% endmacro %}