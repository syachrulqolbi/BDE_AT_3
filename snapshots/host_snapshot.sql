{% snapshot host_snapshot %}

{{
    config(
        strategy='timestamp',
        unique_key='host_id',
        updated_at='scraped_date',
        alias='host'
    )
}}

with filtered as (
    select
        to_timestamp(scraped_date, 'yy/mm/dd')::timestamptz as scraped_date,
        host_id::int,
        NULLIF(host_name, 'NaN')::varchar as host_name,
        to_timestamp(NULLIF(host_since, 'NaN'), 'dd/mm/yy') as host_since,
        NULLIF(host_is_superhost, 'NaN')::varchar as host_is_superhost,
        row_number() over (partition by host_id order by to_timestamp(scraped_date, 'yy/mm/dd') desc) as row_num
    from {{ ref('b_host') }}
)

select 
    scraped_date,
    host_id,
    host_name,
    host_since,
    host_is_superhost
from filtered
where row_num = 1

{% endsnapshot %}