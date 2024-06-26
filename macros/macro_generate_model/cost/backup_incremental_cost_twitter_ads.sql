{%- macro backup_incremental_cost_twitter_ads(dataset, ref_model) -%}
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
            'ad_account_id',
            'adgroup_id',
            'campaign_id',
            'product_type',
            'segment_value',
            'ios_app_store_identifier',
            'android_app_store_identifier'
        )}} as primary_key,

        t1.* except(
            _airbyte_raw_id,
            _airbyte_extracted_at, 
            _airbyte_meta,
            date
        ),
        date(t1.date) as date,

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