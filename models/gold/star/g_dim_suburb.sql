{{
    config(
        unique_key='suburb_name',
        alias='dim_suburb'
    )
}}

select
	*
from {{ ref('s_suburb') }}