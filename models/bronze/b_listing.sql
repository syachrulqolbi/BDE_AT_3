{{
    config(
        unique_key='listing_id',
        alias='listing'
    )
}}

select 
    listing_id,
    scrape_id,
    scraped_date,
    host_id,
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
from {{ source('raw', 'raw_listing') }}