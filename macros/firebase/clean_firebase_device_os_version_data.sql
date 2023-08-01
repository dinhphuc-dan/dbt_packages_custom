{% macro clean_firebase_device_os_version_data(check_os_version_column, prefix_colum = 'device.operating_system' ,condition = var('device_os_system') ) %}
    
    {% if condition == 'android_only' %}
        {{clean_android_device_os_version_data(check_os_version_column)}}
    {% elif condition == 'both_android_and_ios' or condition == 'ios_only' %}
        case when
            if(REGEXP_CONTAINS(REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9].[0-9]){1}'), '[.]'), concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9].[0-9]){1}')), if(REGEXP_CONTAINS(REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9]){1}'), '[.]'),concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9]){1}'),'.0'),concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '[0-9]{1,2}'),'.0.0'))) is null
            or 
            if(REGEXP_CONTAINS(REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9].[0-9]){1}'), '[.]'), concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9].[0-9]){1}')), if(REGEXP_CONTAINS(REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9]){1}'), '[.]'),concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9]){1}'),'.0'),concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '[0-9]{1,2}'),'.0.0'))) = ''
        then 'unknown'
        else
            if(REGEXP_CONTAINS(REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9].[0-9]){1}'), '[.]'), concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9].[0-9]){1}')), if(REGEXP_CONTAINS(REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9]){1}'), '[.]'),concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '([0-9]{1,2}.[0-9]){1}'),'.0'),concat({{prefix_colum}},' ',REGEXP_EXTRACT({{check_os_version_column}}, '[0-9]{1,2}'),'.0.0')))
        end
    {% endif %}
{% endmacro %}