{{
    config(
        unique_key='lga_code',
        alias='dim_lga'
    )
}}

select
	*
from {{ ref('s_lga') }}