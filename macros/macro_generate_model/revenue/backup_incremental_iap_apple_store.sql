{%- macro backup_incremental_iap_apple_store_sale(dataset, ref_model) -%}
{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    on_schema_change="append_new_columns",
    schema = dataset,
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ['date', 'primary_key'],
    labels = {'backup':'revenue'},
)}}

with t1 as 
(
    select
        *,
        cast(Apple_Identifier as  STRING) as Apple_Identifier_string,
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
            'Begin_Date', 
            'Category', 
            'Developer', 
            'Apple_Identifier_string',
            'Customer_Currency', 
            'Order_Type', 
            'Version', 
            'Provider_Country', 
            'CMB', 
            'Client', 
            'Subscription', 
            'Device', 
            'Period', 
            'Provider', 
            'Supported_Platforms', 
            'Product_Type_Identifier', 
            'Country_Code', 
            'Currency_of_Proceeds',
            'Preserved_Pricing',
            'Proceeds_Reason', 
            'SKU'
        )}} as primary_key,

        t1.* except(
            _airbyte_raw_id,
            _airbyte_extracted_at, 
            _airbyte_meta,
            Begin_Date,
            End_Date,
            Apple_Identifier_string
        ),
        
        {{string_to_date('Begin_Date', '%m/%d/%Y')}} as date,
    from t1
),
t3 as 
(
    select
        rank() over(partition by primary_key order by airbyte_emitted_date desc) as ranking,
        t2.*
    from t2 
    {% if is_incremental() %}
    where date >= date_sub(current_date, interval {{var('number_days_backwards')}} day)
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