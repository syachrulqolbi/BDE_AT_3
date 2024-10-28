{{
    config(
        unique_key='lga_code_2016',
        alias='dim_census_g02'
    )
}}

select
	*
from {{ ref('s_census_g02') }}