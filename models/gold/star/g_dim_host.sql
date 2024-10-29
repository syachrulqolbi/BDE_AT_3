{{
    config(
        unique_key='host_id',
        alias='dim_host'
    )
}}

with

source  as (

    select * from {{ ref('host_snapshot') }}

),

cleaned as (
    select
        scraped_date,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),

unknown as (
    select
        null::timestamp as scraped_date,
        0::int as host_id,
        null::varchar as host_name,
        null::timestamp as host_since,
        null::varchar as host_is_superhost,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)
select * from unknown
union all
select * from cleaned