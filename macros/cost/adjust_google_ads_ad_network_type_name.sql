{% macro adjust_google_ads_ad_network_type_name(column_name) %}
    case
        when {{column_name}} = 'SEARCH' then 'Google search'
        when {{column_name}} = 'SEARCH_PARTNERS' then 'Search partners'
        when {{column_name}} = 'CONTENT' then 'Display Network'
        when {{column_name}} = 'YOUTUBE_SEARCH' then 'YouTube Search'
        when {{column_name}} = 'YOUTUBE_WATCH' then 'YouTube Videos'
        when {{column_name}} = 'MIXED' then 'Cross-network'
        else lower({{column_name}})
    end as ad_network_type
{%- endmacro %}