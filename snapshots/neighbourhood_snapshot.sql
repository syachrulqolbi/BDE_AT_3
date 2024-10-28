{% snapshot neighbourhood_snapshot %}

{{
    config(
        strategy='timestamp',
        unique_key='listing_id',
        updated_at='scraped_date',
        alias='neighbourhood'
    )
}}

with filtered as (
    select
        to_timestamp(scraped_date, 'yy/mm/dd') as scraped_date,
        listing_id::int,
        NULLIF(host_neighbourhood::varchar, 'NaN') as host_neighbourhood,
        NULLIF(listing_neighbourhood::varchar, 'NaN') as listing_neighbourhood,
        row_number() over (partition by listing_id order by to_timestamp(scraped_date, 'yy/mm/dd') desc) as row_num
    from {{ ref('b_neighbourhood') }}
)

select 
    scraped_date,
    listing_id,
    host_neighbourhood,
    listing_neighbourhood
from filtered
where row_num = 1

{% endsnapshot %}