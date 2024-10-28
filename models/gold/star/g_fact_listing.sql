{{
    config(
        unique_key='listing_id',
        alias='fact_listing'
    )
}}

select
	listing_id,
    scrape_id,
    scraped_date,
    case when host_id in (select distinct host_id from {{ ref('g_dim_host') }}) then host_id else 0 end as host_id,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
from {{ ref('s_listing') }}