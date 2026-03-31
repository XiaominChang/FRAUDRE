-- -------------------------------------------------------------------------------------------------
-- Author:      Anahita Namvar
-- Description: HUON Policy Transactions Extract from EDH - CTP                                                                                
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- VERSIONS   DATE        WHO         	      DESCRIPTION
-- 1.0        23/02/2024  Anahita Namvar   Initial version.
-- 2.0        19/03/2024  Anahita Namvar   V1
-- -------------------------------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['policy_number'],
          post_hook = grant_access(this)
          ) }}
          
with policy_term_first_transaction as (
select distinct on  (policy_number_extended, effective_from_date, effective_to_date) *
from  ctx.mv_huon_pi_policy_transaction_ctp_extn
--where  to_date(policy_term_inception_date::character VARYING(8),'yyyymmdd') <= '2023-06-01'
--       and to_date(policy_renewal_due_date::character VARYING(8),'yyyymmdd') >= '2018-01-01'
where  to_date(policy_term_inception_date::character VARYING(8),'yyyymmdd') <= '{{ var("end_date") }}' 
       and to_date(policy_renewal_due_date::character VARYING(8),'yyyymmdd') >= '{{ var("start_date") }}' 
       and transaction_type_code NOT IN ('0300','0310')
order by policy_number_extended,
         effective_from_date,
         effective_to_date,
         business_transaction_number ASC,
         effective_from_date,
         effective_to_date
)
,

lagstartenddate_policy_term_first_transaction_leadstartenddate as (
SELECT *,
lead(to_date(effective_from_date::character VARYING(8),'yyyymmdd')) OVER (partition BY policy_number_extended ORDER BY policy_number_extended, effective_from_date,effective_to_date) AS next_effective_from_date_x ,
lead(to_date(effective_to_date::character VARYING(8),'yyyymmdd')) OVER (partition BY policy_number_extended ORDER BY policy_number_extended, effective_from_date,effective_to_date) AS next_effective_to_date_x ,
lag(to_date(effective_from_date::character VARYING(8),'yyyymmdd')) OVER (partition BY policy_number_extended ORDER BY policy_number_extended, effective_from_date,effective_to_date) AS prev_effective_from_date_x ,
lag(to_date(effective_to_date::character VARYING(8),'yyyymmdd')) OVER (partition BY policy_number_extended ORDER BY policy_number_extended, effective_from_date,effective_to_date) AS prev_effective_to_date_x
FROM policy_term_first_transaction
),

huon_policies as (
select distinct
   source_system AS policy_source_system
   , policy_id
   , policy_number
   , policy_number_extended
   , policy_product_code
   , policy_status_code
   , product_class_code
   , sub_company_code
   , business_transaction_number
   , transaction_date
   , transaction_type_code 
   , transaction_net_amount
   , transaction_gross_amount
   , transaction_basic_premium_amount
   , transaction_basic_premium_gst_amount
   , transaction_stamp_duty_amount
   , transaction_stamp_duty_gst_amount
   , transaction_commission_amount
   , transaction_commission_gst_amount
   , discount_percentage_time_of_transaction
   , premium_override_reason_transaction
   , number_changes_per_transaction
   , to_date(policy_inception_date::character VARYING(8),'yyyymmdd')        as policy_inception_date
   , to_date(policy_term_inception_date::character VARYING(8),'yyyymmdd')   as policy_term_inception_date
   , to_date(policy_renewal_due_date::character VARYING(8),'yyyymmdd')      as policy_renewal_due_date
   , to_date(effective_from_date::character VARYING(8),'yyyymmdd')          as effective_from_date
   , to_date(effective_to_date::character VARYING(8),'yyyymmdd')            as effective_to_date
   , lead(to_date(effective_from_date::character VARYING(8),'yyyymmdd')) OVER (partition BY policy_number_extended ORDER BY policy_number_extended, effective_from_date) AS next_effective_from_date
   , registration_number
   , chassis_number
   , vehicle_make 
   , vehicle_model
   , vehicle_body_type_code 
   , engine_capacity
   , year_of_manufacture
   , write_off_flag_id
   , billing_plan_code
   , policy_count 
   , years_of_insurance_count 
   , no_of_claim_free_years
   , driving_experience
   , type_of_ncb
   , risk_address_city_name 
   , risk_address_postcode
   , total_number_of_non_recoverable_claims
   , vehicle_series
   , age_of_the_oldest_driver
   ,age_of_the_youngest_driver
   , risk_state_code
--age related columns
   , gender_youngest_policyholder
   , gender_youngest_driver
   , gender_oldest_policyholder
	, birthdate_youngest_policyholder
	, birthdate_youngest_driver
	, birthdate_oldest_driver
	, birthdate_oldest_policyholder
	, huon_id_youngest_policyholder
	, huon_id_youngest_driver
	, huon_id_oldest_driver
	, huon_id_oldest_policyholder
	, code_source_oldest_driver
	, code_source_oldest_policyholder
	, code_source_youngest_driver
	, code_source_youngest_policyholder
	, business_transaction_number_bpt055
	, business_transaction_number_item
    , 	case 
         when sub_company_code = '64' then 'STATE'
         when sub_company_code in ('100','200') then 'NRMA'
         when sub_company_code = '300' then 'RACV'
         when sub_company_code = '500' then 'SGIC'
         when sub_company_code = '600' then 'SGIO'
         else null
      end as policy_brand
from lagstartenddate_policy_term_first_transaction_leadstartenddate
where not ((prev_effective_from_date_x is not null 
            and to_date(effective_from_date::character VARYING(8),'yyyymmdd') between prev_effective_from_date_x and prev_effective_to_date_x 
            and to_date(effective_to_date::character VARYING(8),'yyyymmdd') between prev_effective_from_date_x and prev_effective_to_date_x) 
          or
           (next_effective_from_date_x is not null 
            and to_date(effective_from_date::character VARYING(8),'yyyymmdd') between next_effective_from_date_x and next_effective_to_date_x 
            and to_date(effective_to_date::character VARYING(8),'yyyymmdd') between next_effective_from_date_x and next_effective_to_date_x))
            
--   The below columns have been removed. They are available for motor but couldn't be found for CTP.
--   , vehicle_category_code
--   , vehicle_engine_type_code 
--   , vehicle_finance_type_code
--   , vehicle_usage_type_code
--   , previous_insurer
--   , parking_type_code
--   , flag_changes_made_to_the_vehicle
--   , total_amount_for_modifications
--   , total_sum_insured
--   , agreed_or_market_value_sum_insured
--   , retail_value_of_the_insured_vehicle
--   , policy_repairer_option_code
--   , loyalty_discount_flag
--   , basic_excess_amount_for_this_period_of_insurance
--   , inexperienced_driver_excess
--   , policy_hire_car_option_code
--   , policy_hire_car_option_description
--   , vehicle_multi_policy_discount_code
--   , current_no_claim_bonus_percentage
--   , age_of_the_youngest_policyholder
--   , vehicle_youngest_owner_gender_code
--   , vehicle_youngest_driver_gender_code
--   , policy_last_incident_rating_code
--   , time_in_months_since_the_last_claim
--   , claims_last_5_years_at_fault_collision
--   , claims_last_5_years_not_at_fault_collision
--   , claims_last_5_years_other
--   , claims_last_5_years_storm
--   , claims_last_5_years_theft
--   , imposed_excess_amount
--   , vehicle_average_kilometres
--   , policy_windscreen_option_code
)  ,
   

huon_cancel as (
select 
   policy_number_extended,
   policy_id,
   business_transaction_number,
   to_date(effective_from_date::character VARYING(8),'yyyymmdd') AS lapse_cancel_effc_from_date,
   to_date(effective_to_date::character VARYING(8),'yyyymmdd')   AS lapse_cancel_effc_to_date,
   to_date(transaction_date::character VARYING(8),'yyyymmdd')    AS lapse_cancel_tran_date
from   ctx.mv_huon_pi_policy_transaction_ctp_extn 
where  transaction_type_code IN ('0300','0310') 
   -- lapse_cancel_reason_code 
),

-- No CTP Data in The table (ctx.huon_pi_daflog) 
-- Demand adjustment factor
-- This table (ctx.huon_pi_daflog) contains the code to tell whether the policy is a brand new one or one that was cancelled some time ago and returning-

--huon_daf as (
--select 
--   distinct policy ,
--   first_value(product) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC)                                    AS product ,
--   first_value(demand_adjustment_factor_policy_match) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC)      AS demand_adjustment_factor_policy_match ,
--   first_value(demand_price_indicator) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC)                     AS demand_price_indicator ,
--   first_value(geoid) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC)                                      AS geoid ,
--   first_value(demand_adjustment_factor_policy_match_date) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC) AS demand_adjustment_factor_policy_match_date ,
--   first_value(effectdte) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC)                                  AS effectdte ,
--   first_value(active_to_date) OVER (partition BY policy, effectdte ORDER BY policy, effectdte, transaction_count DESC)                             AS active_to_date
--from (
--      select 
--         distinct policy ,
--         product ,
--         demand_adjustment_factor_policy_match ,
--         demand_price_indicator ,
--         geoid ,
--         to_date(demand_adjustment_factor_policy_match_date::character VARYING(8),'YYYYMMDD') AS demand_adjustment_factor_policy_match_date ,
--         to_date(effectdte::character VARYING(8),'YYYYMMDD')  AS effectdte ,
--         case
--            when active_to_date=99999999 THEN to_date('99990101','YYYYMMDD')
--            else to_date(active_to_date::character VARYING(8),'YYYYMMDD')
--         end as active_to_date ,
--         transaction_count
--      from      ctx.huon_pi_daflog
--      where     product = 'MOT'
--      order by  transaction_count
--     ) daflog
--),

huon_policies_mod as 
(
select *,
   case
     when huon_policies.effective_to_date>=huon_policies.next_effective_from_date then huon_policies.next_effective_from_date - interval '1 day'
     else huon_policies.effective_to_date
   end::date AS new_effective_to_date
from  huon_policies
order by policy_number
) , 

policy_transaction_huon_dbt as (
select
   chp.*,  
--    greatest(effective_from_date, '2018-01-01' ) AS exposure_start,
--   least(new_effective_to_date, '2023-06-01' , lapse_cancel_effc_from_date) AS exposure_end,
   greatest(effective_from_date, '{{ var("start_date") }}' ) AS exposure_start,
   least(new_effective_to_date, '{{ var("end_date") }}' , lapse_cancel_effc_from_date) AS exposure_end,
   huon_cancel.business_transaction_number AS lapse_cancel_bus_tran_no,
   huon_cancel.lapse_cancel_effc_from_date,
   huon_cancel.lapse_cancel_effc_to_date,
   huon_cancel.lapse_cancel_tran_date
from  huon_policies_mod chp
left join huon_cancel
   on  chp.policy_id = huon_cancel.policy_id
       and (huon_cancel.lapse_cancel_effc_from_date > chp.effective_from_date
            and (huon_cancel.lapse_cancel_effc_from_date < chp.next_effective_from_date or chp.next_effective_from_date is null))
where chp.effective_from_date<=chp.new_effective_to_date      
--      and effective_from_date <= '2023-06-01'
--      and least(new_effective_to_date,lapse_cancel_effc_from_date) >= '2018-01-01' 
      and effective_from_date <= '{{ var("end_date") }}' 
      and least(new_effective_to_date,lapse_cancel_effc_from_date) >= '{{ var("start_date") }}' 
      and (chp.effective_from_date!=chp.next_effective_from_date OR chp.next_effective_from_date IS NULL)
      and (chp.policy_renewal_due_date!=chp.policy_term_inception_date)
      and (chp.effective_from_date>=chp.policy_term_inception_date)
      and (chp.effective_from_date<chp.policy_renewal_due_date) ) , 
     
 huon_final AS (
    SELECT
        policy_number_extended AS policy_number,
        policy_inception_date AS original_policy_inception_date,
        policy_term_inception_date AS policy_term_inception_date,
        policy_term_inception_date AS term_start_date,
        policy_renewal_due_date AS term_end_date,
        effective_from_date AS policy_period_edit_effective_date,
        effective_to_date AS policy_period_expiration_date,
        transaction_net_amount,
        transaction_gross_amount,
        discount_percentage_time_of_transaction,
	    premium_override_reason_transaction,
	    number_changes_per_transaction,
        CASE 
            WHEN transaction_type_code IN (
                                            '0100', -- 'AMENDMENTS - NO PREMIUM'
                                            '0101', -- 'AMENDMENTS - NO PREMIUM REVERSAL OF ORIGINAL.'
                                            '0102', -- 'AMENDMENTS - NO PREMIUM REVERSAL OF TRANSACTION.'
                                            '0110', -- 'AMENDMENTS - EXTRA PREMIUM'
                                            '0111', -- 'AMENDMENTS - EXTRA PREMIUM REVERSAL OF ORIGINAL.'
                                            '0112', -- 'AMENDMENTS - EXTRA PREMIUM REVERSAL OF TRANSACTIONS.'
                                            '0120', -- 'AMENDMENTS - RETURN PREMIUM'
                                            '0121', -- 'AMENDMENTS - RETURN PREMIUM REVERSAL OF ORIGINAL.'
                                            '0122'  -- 'AMENDMENTS - RETURN PREMIUM REVERSAL OF TRANSACTION.' 
                                            ) THEN 'PolicyChange'
            WHEN transaction_type_code IN (
                                            '0020', -- 'POLICY - BINDER'
                                            '0040', -- 'POLICY - NEW BUSINESS'
                                            '0050', -- 'POLICY - QUOTES'
                                            '0051'  -- 'POLICY - QUOTE MAINTENANCE'
                                            ) THEN 'Submission' -- CHECK SUBMISSION CODE MAPPING
            WHEN transaction_type_code IN (
                                            '0162', -- 'RENEWAL REVIEW - REVERSAL OF ORIGINAL'
                                            '0200', -- 'RENEWALS'
                                            '0201', -- 'RENEWALS - REVERSAL OF ORIGINAL.'
                                            '0210'	 -- 'RENEWALS - NOTICE PRODUCED'
                                            ) THEN 'Renewal'
            WHEN transaction_type_code IN (
                                            '2550', -- 'POLICY - REINSTATEMENT.'
                                            '2551'  -- 'REINSTATEMENT - REVERSAL OF ORIGINAL'
                                            ) THEN 'Reinstatement'
            WHEN transaction_type_code IN (
                                            '0300', -- 'CANCELLATIONS - POLICY.'
                                            '0301' -- 'CANCELLATIONS - REVERSAL OF POLICY ORIGINAL.'
                                            ) THEN 'Cancellation'
            ELSE NULL
        END AS job_code,
        business_transaction_number AS job_number, --need to consider subsequent mapping
        CASE
            WHEN billing_plan_code = 'MMTH' THEN 'monthly'
            ELSE 'everyyear'
        END AS billing_period_frequency_code,
        transaction_basic_premium_amount + transaction_basic_premium_gst_amount AS transaction_base_premium,
        transaction_stamp_duty_amount + transaction_stamp_duty_gst_amount AS transaction_stamp_duty,
        transaction_commission_amount + transaction_commission_gst_amount AS transaction_commission,
        lapse_cancel_tran_date AS transaction_date, -- check
        next_effective_from_date AS next_policy_period_edit_effective_date,
        new_effective_to_date AS new_policy_period_expiration_date,
        lapse_cancel_effc_from_date AS cancel_lapse_date,
        registration_number,
            chassis_number AS vehicle_identification_number,
            vehicle_make,
            vehicle_model,
            year_of_manufacture,
            vehicle_body_type_code AS vehicle_shape,
        vehicle_series,
        CAST(engine_capacity AS text) AS engine_cc,
        CASE 
            WHEN risk_state_code = '01' THEN 'AU_ACT'
            WHEN risk_state_code = '02' THEN 'AU_NSW'
            WHEN risk_state_code = '03' THEN 'AU_VIC'
            WHEN risk_state_code = '04' THEN 'AU_QLD'
            WHEN risk_state_code = '05' THEN 'AU_SA'
            WHEN risk_state_code = '06' THEN 'AU_WA'
            WHEN risk_state_code = '07' THEN 'AU_TAS'
            WHEN risk_state_code = '08' THEN 'AU_NT'
            WHEN risk_state_code = '64' THEN 'NZ'
        ELSE ''
        END AS state_code,
        risk_address_city_name AS suburb,
        risk_address_postcode AS postal_code,
          age_of_the_youngest_driver AS age_of_youngest_driver, 
        to_date(transaction_date::character VARYING(8),'yyyymmdd') AS version_transaction_date,
        product_class_code
     
    FROM policy_transaction_huon_dbt
    order by policy_number,original_policy_inception_date,term_start_date,policy_period_edit_effective_date ) 
    
   SELECT 
    policy_number,
    original_policy_inception_date,
    policy_term_inception_date, 
    term_start_date,
    term_end_date,
    policy_period_edit_effective_date,
    policy_period_expiration_date,
    job_code,
    job_number,
    billing_period_frequency_code,
    transaction_base_premium,
    transaction_stamp_duty,
    transaction_commission,
    transaction_date,
    next_policy_period_edit_effective_date,
    new_policy_period_expiration_date,
    cancel_lapse_date,
    registration_number,
    vehicle_identification_number,
    vehicle_make,
    vehicle_model,
    year_of_manufacture,
    vehicle_shape,
    vehicle_series,
    engine_cc,
    state_code,
    suburb,
    postal_code
    ,transaction_net_amount
    ,transaction_gross_amount
    ,discount_percentage_time_of_transaction
    ,premium_override_reason_transaction
    ,number_changes_per_transaction
    ,version_transaction_date
    FROM huon_final


