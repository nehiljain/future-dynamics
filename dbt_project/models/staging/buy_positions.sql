with source as (
    select *
    from {{ source('gha', 'search_itineraries') }}
), additional_cols as (
    select
        source.*,
    from source

)

select * from additional_cols
