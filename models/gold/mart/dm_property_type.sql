{{
    config(
        alias='dm_property_type'
    )
}}

with neighbourhood_lga_map as (
    select 
        n.listing_neighbourhood as host_neighbourhood,
        l.lga_name as host_neighbourhood_lga
    from 
        {{ ref('g_dim_neighbourhood') }} as n
    join 
        {{ ref('g_dim_lga') }} as l on n.listing_neighbourhood = l.lga_name
    where 
        n.valid_to is null  -- Ensure active records in dim_neighbourhood
),

listing_data as (
    select 
        n_lga.host_neighbourhood_lga,
        p.property_type,
        p.room_type,
        p.accommodates,
        date_trunc('month', l.scraped_date) as month_year,
        
        -- active listings and rates
        count(*) filter (where l.has_availability = 't') as active_listings,
        count(*) as total_listings,
        (count(*) filter (where l.has_availability = 't') * 100.0 / nullif(count(*), 0)) as active_listing_rate,
        
        -- price metrics for active listings
        min(l.price) filter (where l.has_availability = 't') as min_price,
        max(l.price) filter (where l.has_availability = 't') as max_price,
        percentile_cont(0.5) within group (order by l.price) filter (where l.has_availability = 't') as median_price,
        avg(l.price) filter (where l.has_availability = 't') as avg_price,
        
        -- distinct hosts
        count(distinct h.host_id) as distinct_hosts,

        -- estimated revenue per host
        coalesce(sum((30 - l.availability_30) * l.price) filter (where l.has_availability = 't'), 0) as estimated_revenue,
        coalesce(sum((30 - l.availability_30) * l.price) filter (where l.has_availability = 't') / nullif(count(distinct h.host_id), 0), 0) as estimated_revenue_per_host

    from 
        {{ ref('g_fact_listing') }} as l
    join 
        {{ ref('g_dim_property') }} as p on l.listing_id = p.listing_id
        and p.valid_to is null  -- Ensure active records in g_dim_property
    left join 
        {{ ref('g_dim_host') }} as h on l.host_id = h.host_id
        and h.valid_to is null  -- Ensure active records in g_dim_host
    left join 
        neighbourhood_lga_map as n_lga on h.host_neighbourhood = n_lga.host_neighbourhood

    group by 
        n_lga.host_neighbourhood_lga, 
        p.property_type, 
        p.room_type, 
        p.accommodates, 
        month_year
)

select * 
from listing_data
order by 
    host_neighbourhood_lga, 
    month_year