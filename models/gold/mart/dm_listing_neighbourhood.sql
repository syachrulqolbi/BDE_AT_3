select 
    dn.listing_neighbourhood,
    date_trunc('month', fl.scraped_date) as month_year,
    
    -- Metrics for Active Listings
    case 
        when count(distinct fl.listing_id) = 0 then 0
        else count(distinct case when fl.has_availability = 't' then fl.listing_id end) * 100.0 / count(distinct fl.listing_id)
    end as active_listing_rate,
    min(case when fl.has_availability = 't' then fl.price end) as min_price_active,
    max(case when fl.has_availability = 't' then fl.price end) as max_price_active,
    percentile_cont(0.5) within group (order by case when fl.has_availability = 't' then fl.price end) as median_price_active,
    cast(avg(case when fl.has_availability = 't' then fl.price end) as decimal(10, 2)) as avg_price_active,
    
    -- Superhost Rate
    cast(
        case 
            when count(distinct dh.host_id) = 0 then 0
            else count(distinct case when dh.host_is_superhost = 't' then dh.host_id end) * 100.0 / count(distinct dh.host_id)
        end 
        as decimal(10, 2)
    ) as superhost_rate,
    
    -- Average Review Score Rating for Active Listings
    cast(avg(case when fl.has_availability = 't' then fl.review_scores_rating end) as decimal(10, 2)) as avg_review_scores_rating,
    
    -- Percentage Change Calculations (Month-over-Month)
    cast(
        case 
            when lag(count(distinct case when fl.has_availability = 't' then fl.listing_id end)) over (partition by dn.listing_neighbourhood order by date_trunc('month', fl.scraped_date)) = 0 then 0
            else (count(distinct case when fl.has_availability = 't' then fl.listing_id end) - 
                  lag(count(distinct case when fl.has_availability = 't' then fl.listing_id end)) over (partition by dn.listing_neighbourhood order by date_trunc('month', fl.scraped_date))
                 ) * 100.0 / lag(count(distinct case when fl.has_availability = 't' then fl.listing_id end)) over (partition by dn.listing_neighbourhood order by date_trunc('month', fl.scraped_date))
        end as decimal(10, 2)
    ) as pct_change_active_listings,
    
    cast(
        case 
            when lag(count(distinct case when fl.has_availability = 'f' then fl.listing_id end)) over (partition by dn.listing_neighbourhood order by date_trunc('month', fl.scraped_date)) = 0 then 0
            else (count(distinct case when fl.has_availability = 'f' then fl.listing_id end) - 
                  lag(count(distinct case when fl.has_availability = 'f' then fl.listing_id end)) over (partition by dn.listing_neighbourhood order by date_trunc('month', fl.scraped_date))
                 ) * 100.0 / lag(count(distinct case when fl.has_availability = 'f' then fl.listing_id end)) over (partition by dn.listing_neighbourhood order by date_trunc('month', fl.scraped_date))
        end as decimal(10, 2)
    ) as pct_change_inactive_listings,
    
    -- Total Number of Stays and Estimated Revenue for Active Listings
    sum(case when fl.has_availability = 't' then (30 - fl.availability_30) end) as total_stays,
    avg(case when fl.has_availability = 't' then cast((30 - fl.availability_30) * fl.price as decimal(10, 2)) end) as avg_estimated_revenue_active,
    
    -- Estimated Revenue per Host
    cast(
        case 
            when count(distinct dh.host_id) = 0 then 0
            else avg((30 - fl.availability_30) * fl.price) / count(distinct dh.host_id)
        end as decimal(10, 2)
    ) as avg_estimated_revenue_per_host
    
from 
    {{ ref('g_fact_listing') }} fl
join 
    {{ ref('g_dim_neighbourhood') }} dn on fl.listing_id = dn.listing_id
left join 
    {{ ref('g_dim_host') }} dh on fl.host_id = dh.host_id

group by 
    dn.listing_neighbourhood,
    date_trunc('month', fl.scraped_date)
order by 
    dn.listing_neighbourhood,
    month_year
