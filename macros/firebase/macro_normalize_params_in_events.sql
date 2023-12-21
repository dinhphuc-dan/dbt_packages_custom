_{% macro generate_query_call_event_without_param(event_name_list_as_dict, ref_model_name) %}
    {%- set event_name_list = [] -%}
        {%- for item in event_name_list_as_dict -%}
            {%- for key, value in item.items() %}
            {%-do event_name_list.append("'"~ key ~"'")-%}
            {%- endfor-%}
        {%-endfor-%}
    with t1 as 
    (    
    select distinct
        date,
        event.event_timestamp as event_timestamp,
        event.event_name as event_name,
        user_id,
        app_version,
        device_os_version,
        geo_country,
        users_source,
        campaign_name,
        concat(device_marketing_name, ' ', device_model_name) as device,
        {%- if var('device_os_system') == 'both_android_and_ios' %}
        platform,
        {% endif -%}
    from {{ ref_model_name }}
    left join unnest(event_nested) as event
    where
        event.event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
        and lower(app_version) not like '%dev%'
        and lower(app_version) not like '%-b%'
    )
{% endmacro%}

{% macro generate_query_call_event_with_param_then_join(event_name_list_as_dict, ref_model_name) %}
    {%- set event_name_list = [] -%}
    {%- set param_key_list = [] -%}

        {%- for item in event_name_list_as_dict -%}
            {%- for key, value in item.items() -%}
            {%- do event_name_list.append("'"~ key ~"'")-%}
                {%- for j, v in value.items() -%}
                {%- do param_key_list.append(v) -%}
                {%-endfor-%}
            {%-endfor-%}
        {%-endfor-%}
    {%- for param_key in param_key_list | unique | list -%}
        {{ 't_' ~ param_key}} as 
        (    
            select distinct
                date,
                event.event_timestamp as event_timestamp,
                event.event_name as event_name,
                user_id,
                COALESCE(event_params.value.string_value, cast(event_params.value.int_value as STRING), cast(event_params.value.float_value as STRING), cast(event_params.value.double_value as STRING), 'unknown') as {{param_key}}
            from {{ ref_model_name }}
            left join unnest(event_nested) as event
            left join unnest(event.event_params) as event_params
            where
                event.event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
                and event_params.key = '{{param_key}}' 
                and lower(app_version) not like '%dev%'
                and lower(app_version) not like '%-b%'
        ),
    {%-endfor-%}

    t_final as 
    (
    select 
        t1.*,
        {%- for param_key in param_key_list | unique | list %}
            {{ 't_' ~ param_key ~ "." ~ param_key}}
            {%- if not loop.last -%} ,
            {%-endif-%}
        {%-endfor%}
    from t1 
    {%- for param_key in param_key_list | unique | list %}
    left join  {{ 't_' ~ param_key}}
    using(date, event_timestamp, event_name, user_id)
    {%-endfor-%}
    )
{% endmacro%}
