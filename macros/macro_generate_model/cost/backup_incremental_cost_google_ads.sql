{%- macro backup_incremental_cost_google_ads(dataset, ref_model) -%}
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
        cast(segments_date as STRING) as date_in_string,
        cast(campaign_id as  STRING) as campaign_id_string,
        cast(user_location_view_country_criterion_id as STRING) as  user_location_view_country_criterion_id_string,
        cast(customer_id as STRING) as  customer_id_string,
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
            'date_in_string', 
            'campaign_id_string', 
            'ad_group_base_ad_group', 
            'segments_ad_network_type', 
            'user_location_view_country_criterion_id_string',
            'customer_id_string', 
            'customer_currency_code'
        )}} as primary_key,

        t1.* except(
            _airbyte_raw_id,
            _airbyte_extracted_at, 
            _airbyte_meta, 
            segments_date, 
            date_in_string, 
            campaign_id_string, 
            user_location_view_country_criterion_id_string, 
            customer_id_string
        ),
        segments_date as date,
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