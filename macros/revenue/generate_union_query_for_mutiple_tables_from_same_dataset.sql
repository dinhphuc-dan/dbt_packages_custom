{% macro generate_union_query_for_mutiple_tables_from_same_dataset(project_id, dataset_id,no_month=none) %}

    {% set from_clause = "`" ~ project_id ~ "." ~ dataset_id ~ "." ~ "INFORMATION_SCHEMA.TABLES" ~"`" %}

    {% set list_table_name_sql %}
        SELECT table_name 
        from {{from_clause}} 
        order by table_name desc 
        {% if no_month is not none %}
        limit {{no_month}}
        {% endif %}
    {% endset %}

    {% if execute %}
    {% set results = run_query(list_table_name_sql) %}
    {% set list_table_name = results.columns[0].values()  %}
    {% else %}
    {% set list_table_name = [] %}
    {% endif %}

    {% for name in list_table_name %}
        select 
            Order_Number, 
            Order_Charged_Date, 
            Order_Charged_Timestamp,
            Financial_Status, 
            Product_Title, 
            Product_ID, 
            Product_Type,
            SKU_ID, 
            Currency_of_Sale, 
            Item_Price,
            Country_of_Buyer
        from `{{[project_id,dataset_id,name] | join('.')}}`
    {% if not loop.last %} union all
    {% endif %}
    {% endfor %}
{% endmacro %}