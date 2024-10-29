{{
    config(
        unique_key='listing_id',
        alias='property'
    )
}}

select 
    listing_id,
    scraped_date,
    property_type,
    room_type,
    accommodates
from {{ source('raw', 'raw_listing') }}