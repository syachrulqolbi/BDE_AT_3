{{
    config(
        unique_key='lga_code_2016',
        alias='census_g02'
    )
}}

with

source  as (

    select 
        (REGEXP_MATCHES(lga_code_2016, '[0-9]+'))[1]::varchar as lga_code_2016,
        NULLIF(median_age_persons, 'NaN')::int as median_age_persons,
        NULLIF(median_mortgage_repay_monthly, 'NaN')::int as median_mortgage_repay_monthly,
        NULLIF(median_tot_prsnl_inc_weekly, 'NaN')::int as median_tot_prsnl_inc_weekly,
        NULLIF(median_rent_weekly, 'NaN')::int as median_rent_weekly,
        NULLIF(median_tot_fam_inc_weekly, 'NaN')::int as median_tot_fam_inc_weekly,
        NULLIF(average_num_psns_per_bedroom, 'NaN')::decimal(10, 2) as average_num_psns_per_bedroom,
        NULLIF(median_tot_hhd_inc_weekly, 'NaN')::int as median_tot_hhd_inc_weekly,
        NULLIF(average_household_size, 'NaN')::decimal(10, 2) as average_household_size
    from {{ ref('b_census_g02') }}

),

renamed as (
    select
        *
    from source
)

select * from renamed