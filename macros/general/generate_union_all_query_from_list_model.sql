{% macro generate_union_all_query_from_list_model(list_app_and_code_as_dict, model_template_name, model_template_name_position = 'Postfix') %}
    {# step 1 - generate list of model name from app name and model template name  #}
    {% set list_models_name = []%}
    {% for item in list_app_and_code_as_dict%}
        {% for code, app_name in item.items() %}
            {% if model_template_name_position.lower() == 'prefix' %}
                {% do list_models_name.append(model_template_name ~ app_name)%}
            {% else %}
                {% do list_models_name.append(app_name ~ model_template_name)%}
            {% endif %}
        {% endfor%}
    {% endfor %}
    {# {{return (list_app_and_code_as_dict)}} #}
    {# step 2 - generate query then union all #}
    {% for model_name in list_models_name %}
        select * from {{ ref(model_name)}} 
        {% if not loop.last %} union all
        {%endif%}
    {% endfor %}
    {% endmacro%}