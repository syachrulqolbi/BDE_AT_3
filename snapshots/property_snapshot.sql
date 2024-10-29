{% snapshot property_snapshot %}

{{
    config(
        strategy='timestamp',
        unique_key='listing_id',
        updated_at='scraped_date',
        alias='property'
    )
}}

with filtered as (
    select
        to_timestamp(scraped_date, 'yy/mm/dd')::timestamptz as scraped_date,
        listing_id::int,
        NULLIF(listing_neighbourhood, 'NaN')::varchar as listing_neighbourhood,
        NULLIF(property_type, 'NaN')::varchar as property_type,
        NULLIF(room_type, 'NaN')::varchar as room_type,
        NULLIF(accommodates, 'NaN')::int as accommodates,
        row_number() over (partition by listing_id order by to_timestamp(scraped_date, 'yy/mm/dd') desc) as row_num
    from {{ ref('b_property') }}
)

select 
    scraped_date,
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates
from filtered
where row_num = 1

{% endsnapshot %}