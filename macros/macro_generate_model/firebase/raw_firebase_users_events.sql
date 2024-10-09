{%- macro raw_firebase_users_events(app_name, dataset, ref_model) -%}
{{ config(materialized='ephemeral') }}
with final as 
(
    select
        {{extract_date_from_timestamp('TIMESTAMP_MICROS(event_timestamp)')}} as date,
        event_timestamp,
        event_name,
        {{generate_user_id('user_pseudo_id','user_first_touch_timestamp','device.mobile_brand_name','device.mobile_model_name','device.language')}},
        user_first_touch_timestamp,
        {{clean_null_and_empty_string('app_info.version')}} as app_version,
        {{clean_null_and_empty_string('device.mobile_brand_name')}} as device_brand_name,
        {{clean_null_and_empty_string('device.mobile_model_name')}} as device_model_name,
        {{clean_null_and_empty_string('device.mobile_marketing_name')}} as device_marketing_name,
        {{clean_firebase_device_os_version_data('device.operating_system_version')}} as device_os_version,
        {{clean_null_and_empty_string('device.language')}} as device_language,
        {{clean_null_and_empty_string('geo.continent')}}  as geo_continent,
        {{clean_null_and_empty_string('geo.country')}} as geo_country,
        {{clean_null_and_empty_string('geo.city')}} as geo_city,
        {{clean_firebase_source_name_data('traffic_source.name')}} as campaign_name,
        {{classify_source_of_users('traffic_source.medium')}} as users_source, 
        {{extract_date_from_timestamp('TIMESTAMP_MICROS(user_first_touch_timestamp)')}} as first_day,
        stream_id,
        lower(platform) as platform,
        event_params
    from {{ref_model}}

    where lower(app_info.version) not like '%-b%' 
        and lower(app_info.version) not like '%dev%' 
        and _TABLE_SUFFIX >= format_date('%Y%m%d', date_sub(date_sub({{ var('date_today')}}, interval {{var('number_days_backwards')}} day), interval 1 day))
)
{% endmacro -%}