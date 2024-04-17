{%- macro backup_incremental_iap_google_store(project, dataset_source, ref_model, dataset_destination) -%}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        on_schema_change = 'append_new_columns',
        schema = dataset_destination,
        partition_by = {
        'field': 'Order_Charged_Date',
        'data_type': 'date',
        'granularity': 'day'
        },
        cluster_by = ['Order_Charged_Date'],
        labels = {'backup':'revenue'},
    )
}}

with t0 as -- just for Lineage Purpose 
(
    select * from {{ref_model}}
),

t1 as 
(
    {{generate_union_query_for_mutiple_tables_from_same_dataset(
        project_id = project,
        dataset_id = dataset_source,
        list_query_column = 'Order_Number, Order_Charged_Date, Order_Charged_Timestamp, Financial_Status, Product_Title, Product_ID, Product_Type, SKU_ID, Currency_of_Sale, Item_Price, Country_of_Buyer'
        )}}
),
final as 
(
    select * from t1
    {% if is_incremental() %}
    where Order_Charged_Date >= date_sub({{ var('date_today')}}, interval {{var('number_days_backwards')}} day)
    {% endif %}
)

{% endmacro -%}