{{
    config(
        unique_key='host_id',
        alias='host'
    )
}}

select 
    scraped_date,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood
from {{ source('raw', 'raw_listing') }}