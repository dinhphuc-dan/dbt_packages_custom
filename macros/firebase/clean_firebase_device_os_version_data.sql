{% macro clean_firebase_device_os_version_data(column_name) %}
    case when
        if(REGEXP_CONTAINS(REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9].[0-9]){1}'), '[.]'), concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9].[0-9]){1}')), if(REGEXP_CONTAINS(REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9]){1}'), '[.]'),concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9]){1}'),'.0'),concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '[0-9]{1,2}'),'.0.0'))) is null
        or 
        if(REGEXP_CONTAINS(REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9].[0-9]){1}'), '[.]'), concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9].[0-9]){1}')), if(REGEXP_CONTAINS(REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9]){1}'), '[.]'),concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9]){1}'),'.0'),concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '[0-9]{1,2}'),'.0.0'))) = ''
    then 'unknown'
    else
        if(REGEXP_CONTAINS(REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9].[0-9]){1}'), '[.]'), concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9].[0-9]){1}')), if(REGEXP_CONTAINS(REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9]){1}'), '[.]'),concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '([0-9]{1,2}.[0-9]){1}'),'.0'),concat(REGEXP_EXTRACT({{column_name}}, '([a-zA-Z]+)'),' ',REGEXP_EXTRACT({{column_name}}, '[0-9]{1,2}'),'.0.0')))
    end
{% endmacro %}