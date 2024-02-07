with base_dates as (
    {{ dbt_date.get_base_dates(start_date="2019-01-01", end_date="2030-01-01") }}
), date_features as  (
    select
        base_dates.date_day,
        {{ dbt_date.date_part("dayofweek", "date_day") }} as day_of_week,
        {{ dbt_date.day_name("date_day", short=true) }} as day_of_week_name,
        {{ dbt_date.iso_week_end("date_day") }} as week_end_date,
        {{ dbt_date.iso_week_of_year("date_day") }} as iso_week_of_year
    from base_dates

)

select * from date_features