{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

select 
    -- Transform listing_neighbourhood to the corresponding LGA
    ln.listing_neighbourhood as host_neighbourhood_lga,
    date_trunc('month', fl.scraped_date) as month_year,

    -- Calculate the number of distinct hosts
    count(distinct fl.host_id) as num_distinct_hosts,

    -- Calculate estimated revenue
    sum(case 
            when fl.has_availability = 't' then (30 - fl.availability_30) * fl.price
            else 0
        end) as estimated_revenue,

    -- Calculate estimated revenue per distinct host
    case 
        when count(distinct fl.host_id) > 0 then
            cast(sum(case 
                        when fl.has_availability = 't' then (30 - fl.availability_30) * fl.price
                        else 0
                    end) / count(distinct fl.host_id) as decimal(10, 2))
        else 0 
    end as estimated_revenue_per_host,

    -- Calculate active listing rate
    (count(case when fl.has_availability = 't' then 1 end) / count(fl.listing_id)) * 100 as active_listing_rate,

    -- Calculate superhost rate
    (count(distinct case when dh.host_is_superhost = 't' then dh.host_id end) / count(distinct dh.host_id)) * 100 as superhost_rate

from 
    {{ ref('g_fact_listing') }} fl
    join {{ ref('g_dim_host') }} dh on fl.host_id = dh.host_id
        and dh.valid_to is null  -- Ensure we join on active records in the g_dim_host table
    join {{ ref('g_dim_neighbourhood') }} ln on fl.listing_id = ln.listing_id
        and ln.valid_to is null  -- Ensure we join on active records in the g_dim_neighbourhood table

where 
    fl.scraped_date is not null
group by 
    host_neighbourhood_lga, month_year
order by 
    host_neighbourhood_lga, month_year
