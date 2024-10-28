{{
    config(
        unique_key='suburb_name',
        alias='suburb'
    )
}}

select
	*
from {{ ref('s_suburb') }}