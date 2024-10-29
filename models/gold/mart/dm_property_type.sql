{{
    config(
        alias='dm_property_type'
    )
}}

select 
    p.property_type,
    p.room_type,
    p.accommodates,
    date_trunc('month', l.scraped_date) as month_year,
    
    -- active listings and rates
    count(case when l.has_availability = 't' then l.listing_id end) as active_listings,
    count(l.listing_id) as total_listings,
    (count(case when l.has_availability = 't' then l.listing_id end) * 100.0 / nullif(count(l.listing_id), 0)) as active_listing_rate,
    
    -- price metrics for active listings
    min(case when l.has_availability = 't' then l.price end) as min_price,
    max(case when l.has_availability = 't' then l.price end) as max_price,
    percentile_cont(0.5) within group (order by case when l.has_availability = 't' then l.price end) as median_price,
    lower(cast(avg(case when l.has_availability = 't' then l.price end) as decimal(10, 2))::text) as avg_price,
    
    -- distinct hosts and superhost rate
    count(distinct h.host_id) as distinct_hosts,
    lower(cast((count(distinct case when h.host_is_superhost = 't' then h.host_id end) * 100.0 / nullif(count(distinct h.host_id), 0)) as decimal(10, 2))::text) as superhost_rate,

    -- average review score for active listings
    lower(cast(avg(case when l.has_availability = 't' then l.review_scores_rating end) as decimal(10, 2))::text) as avg_review_score,
    
    -- monthly percentage change for active and inactive listings
    lower(cast(((count(case when l.has_availability = 't' then l.listing_id end) - 
     lag(count(case when l.has_availability = 't' then l.listing_id end)) over (partition by p.property_type, p.room_type, p.accommodates order by date_trunc('month', l.scraped_date))) * 100.0 / 
     nullif(lag(count(case when l.has_availability = 't' then l.listing_id end)) over (partition by p.property_type, p.room_type, p.accommodates order by date_trunc('month', l.scraped_date)), 0)) as decimal(10, 2))::text) 
    as pct_change_active_listings,

    lower(cast(((count(case when l.has_availability = 'f' then l.listing_id end) - 
     lag(count(case when l.has_availability = 'f' then l.listing_id end)) over (partition by p.property_type, p.room_type, p.accommodates order by date_trunc('month', l.scraped_date))) * 100.0 / 
     nullif(lag(count(case when l.has_availability = 'f' then l.listing_id end)) over (partition by p.property_type, p.room_type, p.accommodates order by date_trunc('month', l.scraped_date)), 0)) as decimal(10, 2))::text) 
    as pct_change_inactive_listings,

    -- number of stays (for active listings)
    sum(case when l.has_availability = 't' then 30 - l.availability_30 end) as num_stays,

    -- estimated revenue per active listing
    lower(cast((sum(case when l.has_availability = 't' then (30 - l.availability_30) * l.price end) / nullif(count(distinct h.host_id), 0)) as decimal(10, 2))::text) as avg_estimated_revenue_per_host
from 
    {{ ref('g_fact_listing') }} as l
join 
    {{ ref('g_dim_property') }} as p on l.listing_id = p.listing_id
left join 
    {{ ref('g_dim_host') }} as h on l.host_id = h.host_id
group by 
    p.property_type, p.room_type, p.accommodates, date_trunc('month', l.scraped_date)
order by 
    p.property_type, p.room_type, p.accommodates, month_year