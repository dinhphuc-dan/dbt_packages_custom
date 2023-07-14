with t1 as 
(
    select * from {{source('raw_incremental_cost_all_apps', 'google_ads_raw_user_location_report')}}
),

final as 
(
    select
        t1.segments_date as date
    from t1
)

select * from final
where final.airbyte_emitted_date in ({{ generate_incremental_date_for_dbt_incremental(number_day_backwards = 7)}})
