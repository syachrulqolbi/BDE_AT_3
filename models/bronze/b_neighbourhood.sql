{{
    config(
        unique_key='listing_id',
        alias='neighbourhood'
    )
}}

select 
    listing_id,
    scraped_date,
    host_neighbourhood,
    listing_neighbourhood
from {{ source('raw', 'raw_listing') }}