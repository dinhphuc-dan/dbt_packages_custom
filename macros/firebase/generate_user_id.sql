{% macro generate_user_id(user_pseudo_id, user_first_touch_timestamp, device_brand_name, device_model_name, device_language) %}
    {{return(adapter.dispatch('generate_user_id')(user_pseudo_id, user_first_touch_timestamp, device_brand_name, device_model_name, device_language))}}
{% endmacro %}

{% macro bigquery__generate_user_id(user_pseudo_id, user_first_touch_timestamp, device_brand_name, device_model_name, device_language) %}
    CASE WHEN user_pseudo_id IS NOT NULL then user_pseudo_id 
    ELSE CONCAT(ifnull({{user_first_touch_timestamp}},0),ifnull({{device_brand_name}},''),ifnull({{device_model_name}},''),ifnull({{device_language}},'')) 
    END as user_id
{%- endmacro %}