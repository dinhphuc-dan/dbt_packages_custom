{%- macro backup_incremental_cost_facebook_ads(dataset, ref_model) -%}
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

with t1 as
(
    select
        *,
        format_date('%Y-%m-%d', date_start) as date_string,
        cast(campaign_id as STRING) as campaign_id_string,
        cast(ad_id as STRING) as ad_id_string,
        cast(adset_id as STRING) as adset_id_string,
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
            'date_string', 
            'country', 
            'account_currency', 
            'campaign_id_string', 
            'ad_id_string', 
            'adset_id_string', 
            'account_id'
        )}} as primary_key,

        t1.* except(
            _airbyte_raw_id,
            _airbyte_extracted_at, 
            _airbyte_meta,
            date_start,
            date_stop, 
            date_string, 
            campaign_id_string, 
            ad_id_string, 
            adset_id_string, 
            unique_actions, 
            unique_outbound_clicks,
            unique_inline_link_clicks,
            outbound_clicks,
            actions
        ),
        
        date_start as date,
        JSON_QUERY_ARRAY(unique_actions) as unique_actions,
        JSON_QUERY_ARRAY(unique_outbound_clicks) as unique_outbound_clicks,
        JSON_QUERY_ARRAY(outbound_clicks) as outbound_clicks,
        JSON_QUERY_ARRAY(actions) as actions,
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