
{% macro generate_query_check_event_param(event_name_list_as_dict) %}
    case
    {%- for item in event_name_list_as_dict -%}
        {%- for event_name, param_list in item.items() %}
        when event_name_in_firebase = '{{event_name}}' 
        then
            case when (
                {%- for key_number, key_name in param_list.items() %}
                {{key_name}} is not null and {{key_name}} <> 'unknown'
                {%- if not loop.last %} and {% endif -%}
                {%-endfor-%}
            ) then 'normal_params' 
            else
                concat(
                    'no_params_key: ',
                    {%- for key_number, key_name in param_list.items() %}
                    if ({{key_name}} is null, '{{key_name}}, ', ''),
                    {%-endfor%}
                    '|| null_params_value: ',
                    {%- for key_number, key_name in param_list.items() %}
                    if ({{key_name}} like '%unknown%', '{{key_name}}, ', '')
                    {%- if not loop.last %} , {% endif -%}
                    {%-endfor%}
                )
            end 
        {%endfor-%}
    {%-endfor%}
    end
    {%- endmacro -%}
