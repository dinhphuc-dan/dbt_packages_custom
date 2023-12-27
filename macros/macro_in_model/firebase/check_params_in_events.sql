
{% macro generate_query_check_event_param(event_name_list_as_dict) %}
    case
    {%- for item in event_name_list_as_dict -%}
        {%- for event_name, param_list in item.items() -%}
            {%- for key_number, key_name in param_list.items() %}
        when event_name_in_firebase = '{{event_name}}' and {{key_name}} is null then concat('{{key_name}}', '; no_params_key')
        when event_name_in_firebase = '{{event_name}}' and {{key_name}} like '%unknown%' then concat('{{key_name}}', '; null_params_value')
            {%-endfor-%}
        {%-endfor-%}
    {%-endfor%}
    else 'normal_params' 
    end
    {%- endmacro -%}
