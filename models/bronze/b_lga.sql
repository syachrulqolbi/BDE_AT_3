{{
    config(
        unique_key='lga_code',
        alias='lga'
    )
}}

select 
    lga_code,
    lga_name
from {{ source('raw', 'raw_lga') }}