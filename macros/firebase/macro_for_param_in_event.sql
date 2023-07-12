{% macro generate_query_call_event_without_param(event_name_list_as_dict, dbt_model_name) %}
    {% set event_name_list = [] %}
        {% for item in event_name_list_as_dict %}
            {% for key, value in item.items() %}
            {% do event_name_list.append("'"~ key ~"'")%}
            {% endfor%}
        {%endfor%}
    with t1 as 
    (    
    select distinct
        date,
        event_timestamp,
        event_name,
        user_id,
        {{clean_null_and_empty_string('app_version')}} as app_version,
        {{clean_android_device_os_version_data('device_os_version')}} as device_os_version,
        {{clean_null_and_empty_string('geo_country')}} as geo_country,
        {{classify_source_of_users('source_medium')}} as users_source,
        {{clean_firebase_source_name_data('source_name')}} as campaign_name
    from {{ ref(dbt_model_name)}}
    where
        event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
        and lower(app_version) not like '%dev%'
    ),
    t2 as 
    (
    select distinct
        date,
        event_timestamp,
        event_name,
        user_id,
        event_params.value.int_value as ga_session_id
    from {{ ref(dbt_model_name)}},
    unnest(event_params) as event_params
    where
    event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
    and lower(app_version) not like '%dev%'
    and event_params.key = 'ga_session_id'
    )
{% endmacro%}

{% macro generate_query_call_event_with_param_then_join(event_name_list_as_dict, dbt_model_name) %}
    {% set event_name_list = [] %}
    {% set param_key_list = [] %}

        {% for item in event_name_list_as_dict %}
            {% for key, value in item.items() %}
            {% do event_name_list.append("'"~ key ~"'")%}
                {% for j, v in value.items() %}
                {% do param_key_list.append(v) %}
                {%endfor%}
            {%endfor%}
        {%endfor%}
    {% for param_key in param_key_list | unique | list %}
        {{ 't_' ~ param_key}} as 
        (    
            select distinct
                date,
                event_timestamp,
                event_name,
                user_id,
                event_params.value.string_value as {{param_key}}
            from {{ ref(dbt_model_name)}},
            unnest(event_params) as event_params
            where
                event_name in {{ '(' ~ event_name_list | join(',') ~ ')'}}
                and event_params.key = '{{param_key}}' 
                and lower(app_version) not like '%dev%'
        ),
    {%endfor%}

    t_final as 
    (
    select 
        t1.*,
        ifnull(t2.ga_session_id, 0) as ga_session_id,
        {% for param_key in param_key_list | unique | list %}
            {{ 't_' ~ param_key ~ "." ~ param_key}}
            {% if not loop.last %} ,
            {%endif%}
        {%endfor%}
    from t1 
    left join t2 
    using(date, event_timestamp, event_name, user_id)
    {% for param_key in param_key_list | unique | list %}
    left join  {{ 't_' ~ param_key}}
    using(date, event_timestamp, event_name, user_id)
    {%endfor%}
    )
{% endmacro%}

