{%- macro raw_firebase_events_params(event_name_list_as_dict, ref_raw_model, ref_screen_button_model) -%}
{{ config(materialized='ephemeral') }}

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
        event_timestamp,
        event_name,
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
    from {{ ref_raw_model }}
    where
        event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
    ),

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
                event_timestamp,
                event_name,
                user_id,
                COALESCE(event_params.value.string_value, cast(event_params.value.int_value as STRING), cast(event_params.value.float_value as STRING), cast(event_params.value.double_value as STRING), 'unknown') as {{param_key}}
            from {{ ref_raw_model }}
            left join unnest(event_params) as event_params
            where
                event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
                and event_params.key = '{{param_key}}' 
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
    ),

d_eventName as 
(
    select * from {{ref_screen_button_model}}
),
t_final_2 as 
(
    select 
        t_final.* except(event_name),
        t_final.event_name as event_name_in_firebase,
        ifnull(d_eventName.screen_feature_button_name, t_final.event_name) as event_name
    from t_final
    left join d_eventName 
    on t_final.event_name = d_eventName.firebase_event_name

),
final as -- new colum params_staus, if event works nomarlly then return value as normal_params, if not return the first param that get issue and its issuse such as no_params or null_params
(
    select
        t_final_2.*,
        {{generate_query_check_event_param(event_name_list_as_dict = event_name_list_as_dict)}} as params_status
    from t_final_2
)

{% endmacro -%}