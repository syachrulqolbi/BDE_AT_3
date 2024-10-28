{{
    config(
        unique_key='lga_code_2016',
        alias='census_g01'
    )
}}

with

source as (

    select 
        lga_code_2016,
        NULLIF(tot_p_m, 'NaN')::int as tot_p_m,
        NULLIF(tot_p_f, 'NaN')::int as tot_p_f,
        NULLIF(tot_p_p, 'NaN')::int as tot_p_p,
        NULLIF(age_0_4_yr_m, 'NaN')::int as age_0_4_yr_m,
        NULLIF(age_0_4_yr_f, 'NaN')::int as age_0_4_yr_f,
        NULLIF(age_0_4_yr_p, 'NaN')::int as age_0_4_yr_p,
        NULLIF(age_5_14_yr_m, 'NaN')::int as age_5_14_yr_m,
        NULLIF(age_5_14_yr_f, 'NaN')::int as age_5_14_yr_f,
        NULLIF(age_5_14_yr_p, 'NaN')::int as age_5_14_yr_p,
        NULLIF(age_15_19_yr_m, 'NaN')::int as age_15_19_yr_m,
        NULLIF(age_15_19_yr_f, 'NaN')::int as age_15_19_yr_f,
        NULLIF(age_15_19_yr_p, 'NaN')::int as age_15_19_yr_p,
        NULLIF(age_20_24_yr_m, 'NaN')::int as age_20_24_yr_m,
        NULLIF(age_20_24_yr_f, 'NaN')::int as age_20_24_yr_f,
        NULLIF(age_20_24_yr_p, 'NaN')::int as age_20_24_yr_p,
        NULLIF(age_25_34_yr_m, 'NaN')::int as age_25_34_yr_m,
        NULLIF(age_25_34_yr_f, 'NaN')::int as age_25_34_yr_f,
        NULLIF(age_25_34_yr_p, 'NaN')::int as age_25_34_yr_p,
        NULLIF(age_35_44_yr_m, 'NaN')::int as age_35_44_yr_m,
        NULLIF(age_35_44_yr_f, 'NaN')::int as age_35_44_yr_f,
        NULLIF(age_35_44_yr_p, 'NaN')::int as age_35_44_yr_p,
        NULLIF(age_45_54_yr_m, 'NaN')::int as age_45_54_yr_m,
        NULLIF(age_45_54_yr_f, 'NaN')::int as age_45_54_yr_f,
        NULLIF(age_45_54_yr_p, 'NaN')::int as age_45_54_yr_p,
        NULLIF(age_55_64_yr_m, 'NaN')::int as age_55_64_yr_m,
        NULLIF(age_55_64_yr_f, 'NaN')::int as age_55_64_yr_f,
        NULLIF(age_55_64_yr_p, 'NaN')::int as age_55_64_yr_p,
        NULLIF(age_65_74_yr_m, 'NaN')::int as age_65_74_yr_m,
        NULLIF(age_65_74_yr_f, 'NaN')::int as age_65_74_yr_f,
        NULLIF(age_65_74_yr_p, 'NaN')::int as age_65_74_yr_p,
        NULLIF(age_75_84_yr_m, 'NaN')::int as age_75_84_yr_m,
        NULLIF(age_75_84_yr_f, 'NaN')::int as age_75_84_yr_f,
        NULLIF(age_75_84_yr_p, 'NaN')::int as age_75_84_yr_p,
        NULLIF(age_85ov_m, 'NaN')::int as age_85ov_m,
        NULLIF(age_85ov_f, 'NaN')::int as age_85ov_f,
        NULLIF(age_85ov_p, 'NaN')::int as age_85ov_p,
        NULLIF(counted_census_night_home_m, 'NaN')::int as counted_census_night_home_m,
        NULLIF(counted_census_night_home_f, 'NaN')::int as counted_census_night_home_f,
        NULLIF(counted_census_night_home_p, 'NaN')::int as counted_census_night_home_p,
        NULLIF(count_census_nt_ewhere_aust_m, 'NaN')::int as count_census_nt_ewhere_aust_m,
        NULLIF(count_census_nt_ewhere_aust_f, 'NaN')::int as count_census_nt_ewhere_aust_f,
        NULLIF(count_census_nt_ewhere_aust_p, 'NaN')::int as count_census_nt_ewhere_aust_p,
        NULLIF(indigenous_psns_aboriginal_m, 'NaN')::int as indigenous_psns_aboriginal_m,
        NULLIF(indigenous_psns_aboriginal_f, 'NaN')::int as indigenous_psns_aboriginal_f,
        NULLIF(indigenous_psns_aboriginal_p, 'NaN')::int as indigenous_psns_aboriginal_p,
        NULLIF(indig_psns_torres_strait_is_m, 'NaN')::int as indig_psns_torres_strait_is_m,
        NULLIF(indig_psns_torres_strait_is_f, 'NaN')::int as indig_psns_torres_strait_is_f,
        NULLIF(indig_psns_torres_strait_is_p, 'NaN')::int as indig_psns_torres_strait_is_p,
        NULLIF(indig_bth_abor_torres_st_is_m, 'NaN')::int as indig_bth_abor_torres_st_is_m,
        NULLIF(indig_bth_abor_torres_st_is_f, 'NaN')::int as indig_bth_abor_torres_st_is_f,
        NULLIF(indig_bth_abor_torres_st_is_p, 'NaN')::int as indig_bth_abor_torres_st_is_p,
        NULLIF(indigenous_p_tot_m, 'NaN')::int as indigenous_p_tot_m,
        NULLIF(indigenous_p_tot_f, 'NaN')::int as indigenous_p_tot_f,
        NULLIF(indigenous_p_tot_p, 'NaN')::int as indigenous_p_tot_p,
        NULLIF(birthplace_australia_m, 'NaN')::int as birthplace_australia_m,
        NULLIF(birthplace_australia_f, 'NaN')::int as birthplace_australia_f,
        NULLIF(birthplace_australia_p, 'NaN')::int as birthplace_australia_p,
        NULLIF(birthplace_elsewhere_m, 'NaN')::int as birthplace_elsewhere_m,
        NULLIF(birthplace_elsewhere_f, 'NaN')::int as birthplace_elsewhere_f,
        NULLIF(birthplace_elsewhere_p, 'NaN')::int as birthplace_elsewhere_p,
        NULLIF(lang_spoken_home_eng_only_m, 'NaN')::int as lang_spoken_home_eng_only_m,
        NULLIF(lang_spoken_home_eng_only_f, 'NaN')::int as lang_spoken_home_eng_only_f,
        NULLIF(lang_spoken_home_eng_only_p, 'NaN')::int as lang_spoken_home_eng_only_p,
        NULLIF(lang_spoken_home_oth_lang_m, 'NaN')::int as lang_spoken_home_oth_lang_m,
        NULLIF(lang_spoken_home_oth_lang_f, 'NaN')::int as lang_spoken_home_oth_lang_f,
        NULLIF(lang_spoken_home_oth_lang_p, 'NaN')::int as lang_spoken_home_oth_lang_p,
        NULLIF(australian_citizen_m, 'NaN')::int as australian_citizen_m,
        NULLIF(australian_citizen_f, 'NaN')::int as australian_citizen_f,
        NULLIF(australian_citizen_p, 'NaN')::int as australian_citizen_p,
        NULLIF(age_psns_att_educ_inst_0_4_m, 'NaN')::int as age_psns_att_educ_inst_0_4_m,
        NULLIF(age_psns_att_educ_inst_0_4_f, 'NaN')::int as age_psns_att_educ_inst_0_4_f,
        NULLIF(age_psns_att_educ_inst_0_4_p, 'NaN')::int as age_psns_att_educ_inst_0_4_p,
        NULLIF(age_psns_att_educ_inst_5_14_m, 'NaN')::int as age_psns_att_educ_inst_5_14_m,
        NULLIF(age_psns_att_educ_inst_5_14_f, 'NaN')::int as age_psns_att_educ_inst_5_14_f,
        NULLIF(age_psns_att_educ_inst_5_14_p, 'NaN')::int as age_psns_att_educ_inst_5_14_p,
        NULLIF(age_psns_att_edu_inst_15_19_m, 'NaN')::int as age_psns_att_edu_inst_15_19_m,
        NULLIF(age_psns_att_edu_inst_15_19_f, 'NaN')::int as age_psns_att_edu_inst_15_19_f,
        NULLIF(age_psns_att_edu_inst_15_19_p, 'NaN')::int as age_psns_att_edu_inst_15_19_p,
        NULLIF(age_psns_att_edu_inst_20_24_m, 'NaN')::int as age_psns_att_edu_inst_20_24_m,
        NULLIF(age_psns_att_edu_inst_20_24_f, 'NaN')::int as age_psns_att_edu_inst_20_24_f,
        NULLIF(age_psns_att_edu_inst_20_24_p, 'NaN')::int as age_psns_att_edu_inst_20_24_p,
        NULLIF(age_psns_att_edu_inst_25_ov_m, 'NaN')::int as age_psns_att_edu_inst_25_ov_m,
        NULLIF(age_psns_att_edu_inst_25_ov_f, 'NaN')::int as age_psns_att_edu_inst_25_ov_f,
        NULLIF(age_psns_att_edu_inst_25_ov_p, 'NaN')::int as age_psns_att_edu_inst_25_ov_p,
        NULLIF(high_yr_schl_comp_yr_12_eq_m, 'NaN')::int as high_yr_schl_comp_yr_12_eq_m,
        NULLIF(high_yr_schl_comp_yr_12_eq_f, 'NaN')::int as high_yr_schl_comp_yr_12_eq_f,
        NULLIF(high_yr_schl_comp_yr_12_eq_p, 'NaN')::int as high_yr_schl_comp_yr_12_eq_p,
        NULLIF(high_yr_schl_comp_yr_11_eq_m, 'NaN')::int as high_yr_schl_comp_yr_11_eq_m,
        NULLIF(high_yr_schl_comp_yr_11_eq_f, 'NaN')::int as high_yr_schl_comp_yr_11_eq_f,
        NULLIF(high_yr_schl_comp_yr_11_eq_p, 'NaN')::int as high_yr_schl_comp_yr_11_eq_p,
        NULLIF(high_yr_schl_comp_yr_10_eq_m, 'NaN')::int as high_yr_schl_comp_yr_10_eq_m,
        NULLIF(high_yr_schl_comp_yr_10_eq_f, 'NaN')::int as high_yr_schl_comp_yr_10_eq_f,
        NULLIF(high_yr_schl_comp_yr_10_eq_p, 'NaN')::int as high_yr_schl_comp_yr_10_eq_p,
        NULLIF(high_yr_schl_comp_yr_9_eq_m, 'NaN')::int as high_yr_schl_comp_yr_9_eq_m,
        NULLIF(high_yr_schl_comp_yr_9_eq_f, 'NaN')::int as high_yr_schl_comp_yr_9_eq_f,
        NULLIF(high_yr_schl_comp_yr_9_eq_p, 'NaN')::int as high_yr_schl_comp_yr_9_eq_p,
        NULLIF(high_yr_schl_comp_yr_8_belw_m, 'NaN')::int as high_yr_schl_comp_yr_8_belw_m,
        NULLIF(high_yr_schl_comp_yr_8_belw_f, 'NaN')::int as high_yr_schl_comp_yr_8_belw_f,
        NULLIF(high_yr_schl_comp_yr_8_belw_p, 'NaN')::int as high_yr_schl_comp_yr_8_belw_p,
        NULLIF(high_yr_schl_comp_d_n_g_sch_m, 'NaN')::int as high_yr_schl_comp_d_n_g_sch_m,
        NULLIF(high_yr_schl_comp_d_n_g_sch_f, 'NaN')::int as high_yr_schl_comp_d_n_g_sch_f,
        NULLIF(high_yr_schl_comp_d_n_g_sch_p, 'NaN')::int as high_yr_schl_comp_d_n_g_sch_p,
        NULLIF(count_psns_occ_priv_dwgs_m, 'NaN')::int as count_psns_occ_priv_dwgs_m,
        NULLIF(count_psns_occ_priv_dwgs_f, 'NaN')::int as count_psns_occ_priv_dwgs_f,
        NULLIF(count_psns_occ_priv_dwgs_p, 'NaN')::int as count_psns_occ_priv_dwgs_p,
        NULLIF(count_persons_other_dwgs_m, 'NaN')::int as count_persons_other_dwgs_m,
        NULLIF(count_persons_other_dwgs_f, 'NaN')::int as count_persons_other_dwgs_f,
        NULLIF(count_persons_other_dwgs_p, 'NaN')::int as count_persons_other_dwgs_p
    from {{ ref('b_census_g01') }}

),

renamed as (
    select
        *
    from source
)

select * from renamed