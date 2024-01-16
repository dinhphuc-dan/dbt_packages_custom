{# This macro merge Campaign and AdGroup report under 1 table #}

{%- macro backup_incremental_cost_apple_search_ads(dataset, campaign_ref_model, adgroup_ref_model) -%}
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
        labels = {'backup':'cost'},
    )
}}

with c1 as -- getting data from campaign report
(
    select 
        *,
        cast(orgId as STRING) as orgId_string,
        cast(campaignId as STRING) as campaignId_string,
        cast(appID as STRING) as appID_string,
        {{extract_date_from_timestamp('_airbyte_extracted_at')}} as airbyte_emitted_date
    from {{campaign_ref_model}}
    {% if is_incremental() %}
    where {{extract_date_from_timestamp('_airbyte_extracted_at')}} in ({{ var('date_today')}},{{ var('date_yesterday')}})
    {% endif %}
),
c2 as 
(
    select
        {{generate_primary_key_table(
            'date', 
            'countriesOrRegions', 
            'displayStatus', 
            'servingStateReasons', 
            'orgId_string',
            'countryOrRegionServingStateReasons', 
            'servingStatus', 
            'countryCode', 
            'campaignStatus', 
            'campaignId_string', 
            'billingEvent', 
            'adChannelType', 
            'appID_string', 
            'supplySources' 
        )}} as primary_key,

        c1.* except(
            _airbyte_raw_id,
            _airbyte_extracted_at, 
            _airbyte_meta,
            orgId_string, 
            campaignId_string, 
            appID_string,
            countryCode,
            date
        ),

         {{string_to_date('date' , '%Y-%m-%d')}} as date,
         ifnull(countryCode, 'unknown') as countryCode,
    from c1 
),
c3 as 
(
    select
        rank() over(partition by primary_key order by airbyte_emitted_date desc) as ranking,
        c2.*
    from c2 
    {% if is_incremental() %}
    where date >= date_sub(current_date, interval {{var('number_days_backwards')}} day)
    {% endif %}
),
c_final as 
(
    select
        c3.* except(ranking)
    from c3 
    where ranking = 1
),

a1 as --getting data from adgroup report
(
    select
        *,
        cast(orgId as STRING) as orgId_string,
        cast(adGroupId as  STRING) as adGroupId_string,
        cast(campaignId as STRING) as campaignId_string,
        cast(automatedKeywordsOptIn as STRING) as automatedKeywordsOptIn_string,
        {{extract_date_from_timestamp('_airbyte_extracted_at')}} as airbyte_emitted_date
    from {{adgroup_ref_model}}
    {% if is_incremental() %}
    where {{extract_date_from_timestamp('_airbyte_extracted_at')}} in ({{ var('date_today')}},{{ var('date_yesterday')}})
    {% endif %}
),
a2 as 
(
    select
        {{generate_primary_key_table(
            'date', 
            'adGroupStatus',
            'orgId_string',
            'adGroupId_string', 
            'countryCode', 
            'adGroupServingStatus', 
            'adGroupDisplayStatus', 
            'pricingModel',
            'campaignId_string',
            'adGroupServingStateReasons', 
            'automatedKeywordsOptIn_string'
        )}} as primary_key,

        a1.* except(
            _airbyte_raw_id,
            _airbyte_extracted_at, 
            _airbyte_meta,
            orgId_string, 
            adGroupId_string, 
            campaignId_string, 
            automatedKeywordsOptIn_string,
            countryCode,
            date
        ),
        {{string_to_date('date' , '%Y-%m-%d')}} as date,
        ifnull(countryCode, 'unknown') as countryCode,
    from a1
),
a3 as 
(
    select
        rank() over(partition by primary_key order by airbyte_emitted_date desc) as ranking,
        a2.*
    from a2 
    {% if is_incremental() %}
    where date >= date_sub(current_date, interval {{var('number_days_backwards')}} day)
    {% endif %}
),
a_final as 
(
    select
        a3.* except(ranking)
    from a3 
    where ranking = 1
),
final as -- mapping campaign and adgroup report altogether
(
    select
        c_final.* except(modificationTime,appID,appName,avgCPT,taps,newDownloads,latOnInstalls,avgCPA,redownloads,avgCPM,latOffInstalls,impressions,conversionRate,installs,ttr,localSpend,primary_key,date,orgID,countryCode,campaignId,airbyte_emitted_date),
        a_final.* except(modificationTime,avgCPT,taps,newDownloads,latOnInstalls,avgCPA,redownloads,avgCPM,latOffInstalls,impressions,conversionRate,installs,ttr,localSpend),
        cast(appID as STRING) as appID,
        ifnull(a_final.taps,c_final.taps) as taps,
        ifnull(a_final.impressions,c_final.impressions) as impressions,
        ifnull(a_final.installs,c_final.installs) as installs,
        ifnull(a_final.newDownloads,c_final.newDownloads) as newDownloads,
        ifnull(a_final.redownloads,c_final.redownloads) as redownloads,
        ifnull(a_final.latOnInstalls,c_final.latOnInstalls) as latOnInstalls,
        ifnull(a_final.latOffInstalls,c_final.latOffInstalls) as latOffInstalls,
        ifnull(a_final.localSpend,c_final.localSpend) as localSpend,
    from a_final
    left join c_final
    using(date, orgID,countryCode,campaignId, airbyte_emitted_date)
)

{% endmacro -%}