{%- macro backup_realtime_iaa_admob_mediation(ref_model) -%}

{{ 
    config(
        materialized='ephemeral',
    ) 
}}
with final as 
(
    select distinct
        {{generate_primary_key_table('APP', 'DATE', 'APP_VERSION_NAME', 'AD_UNIT', 'COUNTRY', 'PLATFORM', 'FORMAT')}} as primary_key,
        APP as admobs_id,
        {{string_to_date('DATE','%Y%m%d')}} as date,
        PLATFORM as platform,
        APP_VERSION_NAME as app_version,
        AD_UNIT as ad_unit_id,
        AD_UNIT_NAME ad_unit_name,
        FORMAT as ad_format, 
        COUNTRY as country_code,
        AD_REQUESTS as ad_requests,
        MATCHED_REQUESTS as matched_requests,
        IMPRESSIONS as impressions,
        ESTIMATED_EARNINGS as estimated_earnings,
        CLICKS as clicks,
    from {{ref_model}}
)
{% endmacro -%}