{{
    config(
        unique_key='suburb_name',
        alias='suburb'
    )
}}

with

source  as (

    select 
        NULLIF(lga_name, 'NaN')::varchar as lga_name,
        NULLIF(suburb_name, 'NaN')::varchar as suburb_name
    from {{ ref('b_suburb') }}

),

renamed as (
    select
        lga_name,
        suburb_name
    from source
)

select * from renamed