{{
    config(
        unique_key='listing_id',
        alias='listing'
    )
}}

with

source  as (

    select 
        listing_id::int,
        NULLIF(scrape_id, 'NaN')::varchar as scrape_id,
        to_timestamp(scraped_date, 'YY/MM/DD') as scraped_date,
        NULLIF(host_id, 'NaN')::int as host_id,
        NULLIF(price, 'NaN')::decimal as price,
        NULLIF(has_availability, 'NaN')::varchar as has_availability,
        NULLIF(availability_30, 'NaN')::int as availability_30,
        NULLIF(number_of_reviews, 'NaN')::int as number_of_reviews,
        NULLIF(review_scores_rating, 'NaN')::decimal as review_scores_rating,
        NULLIF(review_scores_accuracy, 'NaN')::decimal as review_scores_accuracy,
        NULLIF(review_scores_cleanliness, 'NaN')::decimal as review_scores_cleanliness,
        NULLIF(review_scores_checkin, 'NaN')::decimal as review_scores_checkin,
        NULLIF(review_scores_communication, 'NaN')::decimal as review_scores_communication,
        NULLIF(review_scores_value, 'NaN')::decimal as review_scores_value,
        row_number() over (partition by host_id order by to_timestamp(NULLIF(scraped_date, 'NaN'), 'yy/mm/dd') desc) as row_num
    from {{ ref('b_listing') }}

),

renamed as (
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
    from source
    where row_num = 1
)

select * from renamed