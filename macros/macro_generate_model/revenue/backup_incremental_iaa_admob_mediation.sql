{# This file contains 2 independent macro #}
{# Macro 1 #}
{%- macro backup_incremental_iaa_admob_mediation_admob_adsource(dataset, ref_model) -%}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        on_schema_change = 'append_new_columns',
        schema = dataset,
        partition_by = {
        'field': 'date',
        'data_type': 'date',
        'granularity': 'day'
        },
        cluster_by = ['date','primary_key'],
        labels = {'backup':'revenue'},
    )
}}

with t1 as 
(
    select
        *,
        {{extract_date_from_timestamp('_airbyte_extracted_at')}} as airbyte_emitted_date
    from {{ref_model}}
    {% if is_incremental() %}
    where {{extract_date_from_timestamp('_airbyte_extracted_at')}} in ({{ var('date_today')}},{{ var('date_yesterday')}})
    {% endif %}
),
t2 as 
(
    select
        {{generate_primary_key_table(
            'date', 
            'format', 
            'country', 
            'platform', 
            'ad_unit', 
            'app_version_name', 
            'mobile_os_version'
        )}} as primary_key,

        {{string_to_date('t1.date','%Y%m%d')}} as date,
        app as admobs_id,
        format as ad_format,
        country as country_code,
        platform,
        ad_unit as ad_unit_id,
        ad_unit_name,
        '5450213213286189855' as ad_source_id,
        'AdMob Network' as ad_source_name,
        'unknown' as mediation_group_name,
        'unknown' as gma_sdk_version,
        app_version_name as app_version,
        mobile_os_version,
        'unknown' as serving_restriction,
        clicks,
        ad_requests,
        impressions,
        matched_requests,
        estimated_earnings,
        airbyte_emitted_date
    from t1
),
t3 as 
(
    select
        rank() over(partition by primary_key order by airbyte_emitted_date desc) as ranking,
        t2.*
    from t2 
    {% if is_incremental() %}
    where date >= date_sub({{ var('date_today')}}, interval {{var('number_days_backwards')}} day)
    {% endif %}
),
final as 
(
    select
        t3.* except(ranking)
    from t3 
    where ranking = 1
)

{% endmacro -%}

{# Macro 2 #}
{%- macro backup_incremental_iaa_admob_mediation_other_adsources(dataset, ref_model) -%}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        on_schema_change = 'append_new_columns',
        schema = dataset,
        partition_by = {
        'field': 'date',
        'data_type': 'date',
        'granularity': 'day'
        },
        cluster_by = ['date','primary_key'],
        labels = {'backup':'revenue'},
    )
}}


with t1 as 
(
    select
        *,
        {{extract_date_from_timestamp('_airbyte_extracted_at')}} as airbyte_emitted_date
    from {{ref_model}}
    {% if is_incremental() %}
    where {{extract_date_from_timestamp('_airbyte_extracted_at')}} in ({{ var('date_today')}},{{ var('date_yesterday')}})
    {% endif %}
),
t2 as 
(
    select
        {{generate_primary_key_table(
            'date',
            'app',
            'format', 
            'country', 
            'platform', 
            'ad_unit', 
            'ad_source',
            'mediation_group_name',
            'app_version_name',
            'mobile_os_version'
        )}} as primary_key,

        {{string_to_date('t1.date','%Y%m%d')}} as date,
        app as admobs_id,
        format as ad_format,
        country as country_code,
        platform,
        ad_unit as ad_unit_id,
        ad_unit_name,
        ad_source as ad_source_id,
        ad_source_name as ad_source_name,
        mediation_group_name as mediation_group_name,
        'unknown' as gma_sdk_version,
        app_version_name as app_version,
        mobile_os_version,
        'unknown' as serving_restriction,
        clicks,
        ad_requests,
        impressions,
        matched_requests,
        estimated_earnings,
        airbyte_emitted_date
    from t1
),
t3 as 
(
    select
        rank() over(partition by primary_key order by airbyte_emitted_date desc) as ranking,
        t2.*
    from t2 
    {% if is_incremental() %}
    where date >= date_sub({{ var('date_today')}}, interval {{var('number_days_backwards')}} day)
    {% endif %}
),
final as 
(
    select
        t3.* except(ranking)
    from t3 
    where ranking = 1
)

{% endmacro -%}
