{{
    config(
        unique_key='lga_code',
        alias='lga'
    )
}}

with

source  as (

    select 
        NULLIF(lga_code::varchar, 'NaN') as lga_code,
        NULLIF(lga_name::varchar, 'NaN') as lga_name
    from {{ ref('b_lga') }}

),

renamed as (
    select
        *
    from source
)

select * from renamed