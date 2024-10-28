{{
    config(
        unique_key='suburb_name',
        alias='suburb'
    )
}}

select 
    lga_name,
    suburb_name
from {{ source('raw', 'raw_suburb') }}