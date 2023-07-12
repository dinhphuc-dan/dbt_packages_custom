{% macro adjust_firebase_ad_format_name(column_name ) %}
     case 
        when {{column_name}} = 'show_ad_native' then 'native'
        when {{column_name}} in ('show_ad_open_ads','show_ad_open_ads_resume') then 'app_open'
        when {{column_name}} = 'show_ad_rewarded' then 'rewarded'
        when {{column_name}} = 'show_ad_banner' then 'banner'
        when {{column_name}} = 'show_ad_interstitial' then 'interstitial'
    else {{column_name}} 
    end
{% endmacro %}