SELECT 
    dn.listing_neighbourhood,
    DATE_TRUNC('month', fl.scraped_date) AS month_year,
    
    -- Active Listings Rate
    CASE 
        WHEN COUNT(DISTINCT fl.listing_id) = 0 THEN 0
        ELSE COUNT(DISTINCT CASE WHEN fl.has_availability = 't' THEN fl.listing_id END) * 100.0 / COUNT(DISTINCT fl.listing_id)
    END AS active_listing_rate,
    
    -- Pricing for Active Listings
    MIN(CASE WHEN fl.has_availability = 't' THEN fl.price END) AS min_price_active,
    MAX(CASE WHEN fl.has_availability = 't' THEN fl.price END) AS max_price_active,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN fl.has_availability = 't' THEN fl.price END) AS median_price_active,
    CAST(AVG(CASE WHEN fl.has_availability = 't' THEN fl.price END) AS DECIMAL(10, 2)) AS avg_price_active,
    
    -- Superhost Rate Calculation
    CAST(
        CASE 
            WHEN COUNT(DISTINCT dh.host_id) = 0 THEN 0
            ELSE COUNT(DISTINCT CASE WHEN dh.host_is_superhost = 't' THEN dh.host_id END) * 100.0 / COUNT(DISTINCT dh.host_id)
        END AS DECIMAL(10, 2)
    ) AS superhost_rate,
    
    -- Average Review Score for Active Listings
    CAST(AVG(CASE WHEN fl.has_availability = 't' THEN fl.review_scores_rating END) AS DECIMAL(10, 2)) AS avg_review_scores_rating,
    
    -- Percentage Change in Active Listings
    CAST(
        CASE 
            WHEN LAG(COUNT(DISTINCT CASE WHEN fl.has_availability = 't' THEN fl.listing_id END)) OVER (PARTITION BY dn.listing_neighbourhood ORDER BY DATE_TRUNC('month', fl.scraped_date)) = 0 THEN 0
            ELSE (COUNT(DISTINCT CASE WHEN fl.has_availability = 't' THEN fl.listing_id END) - 
                  LAG(COUNT(DISTINCT CASE WHEN fl.has_availability = 't' THEN fl.listing_id END)) OVER (PARTITION BY dn.listing_neighbourhood ORDER BY DATE_TRUNC('month', fl.scraped_date))
                 ) * 100.0 / LAG(COUNT(DISTINCT CASE WHEN fl.has_availability = 't' THEN fl.listing_id END)) OVER (PARTITION BY dn.listing_neighbourhood ORDER BY DATE_TRUNC('month', fl.scraped_date))
        END AS DECIMAL(10, 2)
    ) AS pct_change_active_listings,
    
    -- Total Stays and Revenue for Active Listings
    SUM(CASE WHEN fl.has_availability = 't' THEN (30 - fl.availability_30) END) AS total_stays,
    AVG(CASE WHEN fl.has_availability = 't' THEN CAST((30 - fl.availability_30) * fl.price AS DECIMAL(10, 2)) END) AS avg_estimated_revenue_active,
    
    -- Revenue per Host Calculation
    CAST(
        CASE 
            WHEN COUNT(DISTINCT dh.host_id) = 0 THEN 0
            ELSE AVG((30 - fl.availability_30) * fl.price) / COUNT(DISTINCT dh.host_id)
        END AS DECIMAL(10, 2)
    ) AS avg_estimated_revenue_per_host
    
FROM 
    {{ ref('g_fact_listing') }} fl
JOIN 
    {{ ref('g_dim_neighbourhood') }} dn ON fl.listing_id = dn.listing_id
    AND dn.valid_to IS NULL
LEFT JOIN 
    {{ ref('g_dim_host') }} dh ON fl.host_id = dh.host_id
    AND dh.valid_to IS NULL

GROUP BY 
    dn.listing_neighbourhood,
    DATE_TRUNC('month', fl.scraped_date)
ORDER BY 
    dn.listing_neighbourhood,
    month_year