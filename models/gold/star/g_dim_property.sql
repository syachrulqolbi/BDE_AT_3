{{
    config(
        unique_key='listing_id',
        alias='dim_property'
    )
}}

with

source  as (

    select * 
    from {{ ref('property_snapshot') }}
    where dbt_valid_to is null  -- filter to only keep the latest records

),

cleaned as (
    select
        scraped_date,
        listing_id,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),

unknown as (
    select
        null::timestamp as scraped_date,
        0::int as listing_id,
        null::varchar as listing_neighbourhood,
        null::varchar as property_type,
        null::varchar as room_type,
        0::int as accommodates,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)
select * from unknown
union all
select * from cleaned