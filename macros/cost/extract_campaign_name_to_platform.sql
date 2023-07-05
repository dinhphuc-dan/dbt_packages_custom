{% macro extract_campaign_name_to_platform(column_name) %}
    {{return(adapter.dispatch('extract_campaign_name_to_platform')(column_name))}}
{% endmacro %}

{% macro bigquery__extract_campaign_name_to_platform(column_name) %}
    case
        when trim(lower(regexp_extract({{column_name}}, r'[a-zA-Z0-9.-]+',1,2))," ") NOT IN ('ios', 'and') then 'campaign_not_set_platform'
        else trim(lower(regexp_extract({{column_name}}, r'[a-zA-Z0-9.-]+',1,2))," ") 
    end
{% endmacro %}