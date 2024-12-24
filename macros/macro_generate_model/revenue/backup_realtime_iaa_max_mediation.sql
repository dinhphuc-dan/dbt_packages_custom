{%- macro backup_realtime_iaa_max_mediation(ref_model) -%}

{{ 
    config(
        materialized='ephemeral',
    ) 
}}


with t1 as
(
    select *
    from {{ref_model}}
), 
final as 
(
    select 
        REGEXP_REPLACE(CONCAT(ifnull(country,''),ifnull(platform,''),ifnull(network,''),ifnull(max_ad_unit_id,''),ifnull(package_name,''),ifnull(day,''),ifnull(hour,''),ifnull(ad_format,'')),' ','') as primary_key,
        {{string_to_date('day', '%Y-%m-%d')}} as date,
        parse_datetime('%Y-%m-%d %H:%M', concat(day, ' ', hour)) as datetime,
        package_name,
        platform,
        country as country_code,
        network, 
        ad_format,
        max_ad_unit, 
        application,
        max_ad_unit_id,
        attempts,
        responses,
        impressions,
        estimated_revenue,
    from t1
)
    
{% endmacro -%}
