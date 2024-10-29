{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

select 
    -- transform host_neighbourhood to the corresponding lga
    ln.host_neighbourhood as host_neighbourhood_lga,
    date_trunc('month', fl.scraped_date) as month_year,

    -- calculate number of distinct hosts
    count(distinct fl.host_id) as num_distinct_hosts,

    -- calculate estimated revenue
    sum(case 
            when fl.has_availability = 't' then (30 - fl.availability_30) * fl.price
            else 0
        end) as estimated_revenue,

    -- calculate estimated revenue per host (distinct)
    case 
        when count(distinct fl.host_id) > 0 then
            cast(sum(case 
                        when fl.has_availability = 't' then (30 - fl.availability_30) * fl.price
                        else 0
                    end) / count(distinct fl.host_id) as decimal(10, 2))
        else 0 
    end as estimated_revenue_per_host,

    -- calculate active listing rate
    (count(case when fl.has_availability = 't' then 1 end) / count(fl.listing_id)) * 100 as active_listing_rate,

    -- calculate superhost rate
    (count(distinct case when dh.host_is_superhost = 't' then dh.host_id end) / count(distinct dh.host_id)) * 100 as superhost_rate

from 
    {{ ref('g_fact_listing') }} fl
    join {{ ref('g_dim_host') }} dh on fl.host_id = dh.host_id
    join {{ ref('g_dim_neighbourhood') }} ln on dh.host_neighbourhood = ln.listing_neighbourhood

where 
    fl.scraped_date is not null
group by 
    host_neighbourhood_lga, month_year
order by 
    host_neighbourhood_lga, month_year