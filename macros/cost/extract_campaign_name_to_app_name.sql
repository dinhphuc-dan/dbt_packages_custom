{% macro extract_campaign_name_to_app_name(column_name) %}
    {{return(adapter.dispatch('extract_campaign_name_to_app_name')(column_name))}}
{%- endmacro %}

{% macro bigquery__extract_campaign_name_to_app_name(column_name) %}
    case
        when lower(trim((regexp_extract({{column_name}}, r'[a-zA-Z0-9.-]+',1,1))," ")) = 'ballrun' then 'BallRun2048'
        else trim((regexp_extract({{column_name}}, r'[a-zA-Z0-9.-]+',1,1))," ") 
    end
{%- endmacro %}



