{% macro generate_customer_journey(number_of_step) %}
     {% set list_steps = []  %}
     {% for i in range(0, number_of_step + 1) %}
        {% do list_steps.append("ifnull(LEAD(event_screen_name,"~ i ~") OVER(PARTITION BY date, user_id, ga_session_id ORDER BY event_timestamp),''),'||'")%}
     {% endfor %}
     {{"trim(CONCAT("~ list_steps | join(',') ~"),'||')"}}
{% endmacro %}
