
-- -------------------------------------------------------------------------------------------------
-- Author:      Ana Namvar
-- Description: CTP claims data                                                                                
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- VERSIONS   DATE        WHO         			  DESCRIPTION
-- 1.0        11/01/2024  Ana Namvar  	            Initial version
-- 2.0		  18/03/2024  Ana Namvar  		        v2
-- 3.0        03/04/2024  Xiaomin Chang             v3

{{ config(
          materialized='table',
          distributed_by = ['claim_number'],
          post_hook = grant_access(this)
        ) }}

with cte_claim as 
(
     select
      source_system
    , claim_number
    , claim_id
    , policy_number
    , policy_id as policy_id_clm
    , policy_brand
    , policy_original_inception_date
    , policy_period_start_date
    , policy_period_end_date
    , policy_issue_state
    , policy_line_of_business_name
    , policy_manufacturer_name --use this as a filter to separate personal / commercial lines
    , policy_distribution_channel_type
    , policy_system_type_code
    , policy_system_type_name
    , policy_issue_state_name
    , policy_status_name
    , policy_payment_frequency_name
    , policy_underwriting_name
    , customer_number
    , customer_since_date
    , vulnerable_customer_flag
    , vulnerability_category_name
    , loss_location_id
    , loss_suburb_name
    , claim_loss_state 
    , claim_postcode
    , claim_status_name
    , claim_status_name_derived
    , claim_status_conformed
    , claim_closed_outcome_name
    , claim_decision_status_name
    , closed_outcome_denial_reason_code
    , claim_denial_reason_name
    , closed_outcome_withdrawn_reason_code
    , closed_outcome_withdrawn_reason_name
    , claim_description
    , total_excess_payable
    , general_nature_of_loss_name
    , cause_of_loss_name
    , claim_type_conformed
    , incident_type_conformed
    , peril_type_code
    , line_of_business_name
    , claim_loss_type_name
    , claim_lodgement_team
    , fault_rating_name
    , claim_reported_by_type_name
    , claim_how_reported_name
    , claimant_count
    , CAST(police_involved AS integer) as police_flag
    , police_reported_dt as police_reported_date
    , claim_segment_name
    , policy_valid_for_claim_name
    , notify_only_claim_flag
    , basic_excess_amount
    , deductible_amount
    , waived
    , employee_claim_flag
    , fraud_risk_flag
    , section_one_score
    , refer_claim_to_investigations_team_name
    , investigation_outcome_name
    , fatality
    FROM
        ctx.mv_cc_ci_claim_header_ext
    WHERE 
        claim_status_name <> 'Draft' 
	    and ctp_statutory_insurer_state_name IN ('NSW')
	    and line_of_business_name = 'Compulsory Third Party'
	    and notify_only_claim_flag = 'No'
	    -- and claim_lodgement_date::date BETWEEN  '{{ var("claim_start_date") }}'  AND '{{ var("claim_end_date") }}' 
        and (claim_closed_outcome_name IS NULL OR claim_closed_outcome_name='Completed')
),

  
roles AS 
(
select  
	 a.role_name
	,c.claim_number
from  ctx.mv_cc_ci_claim_contact_ext a 
INNER JOIN  cte_claim AS c 
	on a.claim_number = c.claim_number
), 
 
-- roles_flag as 
-- ( 
-- select 
-- 	c.claim_number
-- 	,contact_id  
-- 	,CASE WHEN role_name='Witness' THEN 1 ELSE 0 END AS witness_flag 
-- 	,CASE WHEN role_name='Insured''s Spouse' THEN 1 ELSE 0 END AS  insured_spouse_flag
-- 	,CASE WHEN role_name='Partner/Spouse' THEN 1 ELSE 0 END AS partner_spouse_flag
-- 	,CASE WHEN role_name='Insured''s Relative' THEN 1 ELSE 0 END AS insured_relative_flag 
-- 	,CASE WHEN role_name='Parent/Guardian' THEN 1 ELSE 0 END AS parent_guardian_flag 
-- 	,CASE WHEN role_name='Family Member' THEN 1 ELSE 0 END AS family_member_flag 
-- 	,CASE WHEN role_name in ('Injured Party', 'Other Injured Party') THEN 1 ELSE 0 END AS injured_party_flag
-- from ctx.mv_cc_ci_claim_contact_ext t1 
-- inner join cte_claim AS c 
-- 	on t1.claim_number = c.claim_number 
	-- ), 


roles_flag as 
( 
    SELECT 
    claim_number, 
    foo.name as name_involved_party,
    CASE WHEN witness1 >0 THEN 1 ELSE 0 END as role_witness_flag,
    CASE WHEN insured > 0 AND driver > 0 THEN 1 ELSE 0 END AS insured_is_driver_flag,
    CASE WHEN spouse > 0 AND driver > 0 THEN 1 ELSE 0 END AS driver_is_insured_spouse_flag,
    CASE WHEN pedestrian > 0 AND injury > 0 THEN 1 ELSE 0 END AS pedestrian_is_injured_flag,
    CASE WHEN witness1>0 and relationship> 0 THEN 1 ELSE 0 END AS witness_has_family_relationship_flag
  FROM (
    SELECT
      a.claim_number,
      a.name, 
      SUM(CASE when role_name = 'Insured' THEN 1 ELSE 0 END) AS insured,
      SUM(CASE when role_name = 'Driver' THEN 1 ELSE 0 END) AS driver,
      SUM(CASE when role_name = 'Insured''s Spouse' THEN 1 ELSE 0 END) AS spouse,
      SUM(CASE when role_name = 'Pedestrian' THEN 1 ELSE 0 END) AS pedestrian,
      SUM(CASE when role_name = 'Injured Party' THEN 1 ELSE 0 END) AS injury,
      SUM(CASE when role_name = 'Witness' THEN 1 ELSE 0 END) AS witness1,
      SUM(CASE WHEN role_name IN ('Partner/Spouse', 'Insured''s Spouse', 'Insured''s Relative', 'Parent/Guardian', 'Family Member') THEN 1 ELSE 0 END) AS relationship
    FROM ctx.mv_cc_ci_claim_contact_ext AS a
    INNER JOIN cte_claim AS c 
      ON a.claim_number = c.claim_number
    GROUP BY 1,2
  ) AS foo
--   GROUP BY 1
	), 

witness_has_family_relationship AS
(
    SELECT DISTINCT 
        t1.claim_number,
        t1.contact_id  
    FROM 
        ctx.mv_cc_ci_claim_contact_ext t1
    WHERE 
        t1.role_name ='Witness'
        AND EXISTS (
            SELECT 1
            FROM 
                ctx.mv_cc_ci_claim_contact_ext t2
            WHERE  
                t2.role_name IN ('Partner/Spouse', 'Insured''s Spouse', 'Insured''s Relative', 'Parent/Guardian', 'Family Member')  
                AND t1.claim_number = t2.claim_number
                AND t1.contact_id = t2.contact_id
        )
) ,

-- driver_status AS (
--   SELECT 
--     claim_number, 
--     SUM(CASE WHEN insured > 0 AND driver > 0 THEN 1 ELSE 0 END) AS insured_is_driver_flag,
--     SUM(CASE WHEN spouse > 0 AND driver > 0 THEN 1 ELSE 0 END) AS driver_is_insured_spouse_flag,
--     SUM(CASE WHEN pedestrian > 0 AND injury > 0 THEN 1 ELSE 0 END) AS pedestrian_is_injured_flag,

--   FROM (
--     SELECT
--       a.claim_number,
--       a.name, 
--       SUM(CASE when role_name = 'Insured' THEN 1 ELSE 0 END) AS insured,
--       SUM(CASE when role_name = 'Driver' THEN 1 ELSE 0 END) AS driver,
--       SUM(CASE when role_name 'Insured''s Spouse' THEN 1 ELSE 0 END) AS spouse
--       SUM(CASE when role_name = 'Pedestrian' THEN 1 ELSE 0 END) AS pedestrian
--       SUM(CASE when role_name = 'Injured Party' THEN 1 ELSE 0 END) AS injury
--     FROM ctx.mv_cc_ci_claim_contact_ext AS a
--     INNER JOIN claim_header AS c 
--       ON a.claim_number = c.claim_number
--     GROUP BY 1,2
--   ) AS foo
--   GROUP BY 1
-- ),


cte_claim_time as(
select 
     claim_number as claim_number2
    ,claim_loss_time
    ,claim_loss_time::date as claim_loss_date
    ,extract('hour' FROM claim_loss_time) AS claim_loss_hour_num
    ,claim_reported_time ::date as claim_report_date
    ,create_time as claim_create_time
    ,create_time::date as claim_lodgement_date
    ,update_time as claim_update_time
    ,claim_finalised_time 
    ,claim_finalised_time::date as claim_finalised_date 
    ,claim_reopen_time
    ,claim_reopen_time::date as claim_reopen_date
from 
	ctx.mv_cc_ci_claim
),
-- losst time - loss hour - date diff - number of updates for a claim - claim reopen time diff claim loss claim lodgment ,

cte_incident as 
(
select 
      claim_id as incident_claim_id 
    , incident_id 
    , vehicle_id as incident_vehicle_id
    , create_time as incident_create_time
    , create_userid as incident_create_userid    
    , update_time as incident_update_time
    , update_userid as incident_update_userid
    , public_id as public_id_inc
    , incident_loss_party_name 
    , alcohol_reading_name 
    , drug_use_name 
    , alcohol_drugs_consumed_name 
    , driver_relation_to_insured_name 
    , incident_severity_name 
    , incident_subtype_name 
    , incident_vehicle_loss_party_name /* this is for vehicle only */
    , incident_vehicle_type_name 
    , incident_vehicle_use_name
    , incident_air_bags_deployed_flag_name 
    , anti_theft_flag_id 
    , incident_comment 
    , time_of_first_drink 
    , last_drink_time
    , type_alcohol_consumed
    , hire_car_daily_rate 
    , hire_care_entitlement 
    , hire_car_maximum_days 
    , hire_car_maximum_limit
    , incident_insured_relation_id 
    , owners_permission_flag_id 
    , incident_property_in_custody_id 
    , incident_purpose_text 
    , salvage_id 
    , value_pre_accident
    , taxi_benefit
    , taxi_fare_limit 
    , taxi_journey_limit 
    , does_the_vehicle_require_a_tow 
    , incident_vehicle_towed_flag_id
    , incident_towing_date 
    , salvage_tow_fees
    , incident_description 
    , incident_equipment_failure_flag
    , odometer_read
    , incident_other_insurer_id
    , incident_recovered_flag_id
    , incident_total_loss_flag_id
    , incident_total_loss_points
    , incident_vehicle_operable_flag_name
    , incident_vehicle_parked_flag_id
    , vehicle_lock_flag_id
    , vehicle_stolen_flag_id
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_one, '0') 
      end as impact_code_one  --front_left_flag 
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_two, '0') 
      end as impact_code_two  --front_right_flag 
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_three, '0') 
      end as impact_code_three --side_right_flag
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_four, '0') 
      end as impact_code_four --rear_right_flag
     , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_five, '0') 
      end as impact_code_five --rear_left_flag 
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_six, '0') 
      end as impact_code_six --side_left_flag 
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_seven, '0') 
      end as impact_code_seven --engine_flag  
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_eight, '0') 
      end as impact_code_eight --roof_flag        
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_nine, '0') 
      end as impact_code_nine  --interior_flag
    , case when coalesce (impact_code_one, impact_code_two, impact_code_three, impact_code_four, impact_code_five, impact_code_six, impact_code_seven, impact_code_eight, impact_code_nine) is NULL then NULL
        else coalesce(impact_code_one, '0') || 
             coalesce(impact_code_two, '0') ||
             coalesce(impact_code_three, '0') ||
             coalesce(impact_code_four, '0') ||
             coalesce(impact_code_five, '0') ||
             coalesce(impact_code_six, '0') ||
             coalesce(impact_code_seven, '0') ||
             coalesce(impact_code_eight, '0') ||
             coalesce(impact_code_nine, '0')
      ::character(9) 
      end as inc_damage_keys_as_01_combinations    
    , no_damage 
    , windscreen_only
    , vehicle_damage_area_windscreen_only
    from 
        ctx.mv_cc_ci_incident mcci 
    where retired=0
),
-- 	select * from  ctx.mv_cc_ci_incident
-- join claim and incident tables 
-- select a.* , b.* from cte_claim a
-- join cte_incident b
-- on b.incident_claim_id=a.claim_id 

cte_vehicle as 
(
    select
      vehicle_id
    , public_id
    , vehicle_rego_number
    , create_time as vehicle_create_time
    , create_userid as vehicle_create_userid
    , update_time as vehicle_update_time
    , update_userid as vehicle_update_userid
    , vehicle_make
    , vehicle_model
    , vehicle_vin
    , vehicle_year_of_manufacture
    , vehicle_class_type_name      
    , vehicle_registration_state_code
    , vehicle_registration_state_name
    , vehicle_value_type_name
    , vehicle_transmission_type_name      
    , if_the_car_coverage_is_there
    , value_of_vehicle
    , vms_id
    , vehicle_colour
    , loan
    , serial_number
    , vehicle_style_code
    , vehicle_style_name   
    , vehicle_body_type_code 
    , vehicle_body_type_name
    , vehicle_make_model_not_available
    from 
        ctx.mv_cc_ci_vehicle
    where retired=0
),

--for CTP this column cte_exposure.exposure_coverage_type_name only have CTP value 
cte_exposure as 
(
    select 
      exposure_id
    , public_id as expr_public_id
    , incident_id as expr_incident_id
    , claimaint_id
    , coverage_id
    , estimate_id
    , exposure_status_name
    , create_time as exposure_create_time
    , create_userid as exposure_create_userid
    , update_time as exposure_update_time
    , update_userid as exposure_update_userid
    , exposure_close_time
    , exposure_closed_outcome_name
    , exposure_reopen_time
    , reopened_reason_name
    , exposure_coverage_type_name
    , exposure_coverage_subtype_name
    , exposure_loss_party_name
    , exposure_type_name
    , exposure_waive_excess_flag_id
    from 
        ctx.mv_cc_ci_exposure
    where retired=0
),

--select distinct exposure_coverage_type_name 
--from cte_claim  a
--join cte_exposure b  
--on a.claim_id = b.claim_id 


cte_riskunit as 
(
select 
      risk_unit_id
    , risk_unit_number as policy_risk_unit_number
    , unit_number as policy_risk_number
    , unit_sequence_number
    , bureau_type_code as type_of_bureau
    , risk_type_id as policy_risk_type_id
    , risk_type_code as policy_risk_type_code
    , risk_type_name as policy_risk_type_name
    , risk_unit_type_id as policy_risk_unit_type_id
    , risk_unit_type_code as policy_risk_unit_type_code
    , risk_unit_type_name as policy_risk_unit_type_name
    , vehicle_location_id
    , policy_location_id
    , policy_risk_pas_key 
    from 
    	ctx.mv_cc_ci_riskunit
    where retired = 0
) ,

cte_history as 
(
    select 
      claim_id 
    , public_id
    , event_time_stamp
    , description
    , history_event_type_code
--  unfortunately the 'from' is missing post 2015-03-27, hence we will never know what the very first one is for claims where the history has been modified
--  , case when history_event_type_code = 'CGU_WtHappned' then regexp_replace(substring(description,'amended from ''(.*)'' to '''), E'[\\n\\r\\u2028]+', ' ', 'g') else NULL end::text as description_before
--  , case when history_event_type_code = 'CGU_WtHappned' then regexp_replace(substring(description,'amended from .* to ''(.*)''.'), E'[\\n\\r\\u2028]+', ' ', 'g') else NULL end::text as description_after
    , case when history_event_type_code = 'CGU_WtHappned' then regexp_replace(substring(description,'amended to ''(.*)''.'), E'[\\n\\r\\u2028]+', ' ', 'g') else NULL end::text as description_amend_to_only
    , case when description ilike '%at fault changed%' then substring(description,'At Fault changed from ''(.*)'' to ''') else NULL end::text as at_fault_amend_from
    , case when description ilike '%at fault changed%' then substring(description,'.* to ''(.*)''') else NULL end::text as at_fault_amend_to
    , case when history_event_type_code = 'CGU_NatureOfLoss' then substring(description,'.* from ''(.*)'' to ''') else NULL end::text as nature_of_loss_amend_from
    , case when history_event_type_code = 'CGU_NatureOfLoss' then substring(description,'.* to ''(.*)''') else NULL end::text as nature_of_loss_amend_to
    , case when history_event_type_code = 'CGU_LossCause' then substring(description,'.* from ''(.*)'' to ''') else NULL end::text as loss_cause_amend_from
    , case when history_event_type_code = 'CGU_LossCause' then substring(description,'.* to ''(.*)''') else NULL end::text as loss_cause_amend_to
    , case when history_event_type_code = 'CGU_DateOfLoss' then substring(description, '.* from (.*) to .*') end::timestamp as claim_loss_time_amend_from
    , case when history_event_type_code = 'CGU_DateOfLoss' then substring(description, '.* to (.*)') end::timestamp as claim_loss_time_amend_to
    , case when history_event_type_code = 'CGU_LossLocation' then regexp_replace(substring(description, '.* from ''(.*)'' to '''), E'[\\n\\r\\u2028]+', ' ', 'g') else NULL end::text as loss_location_amend_from
    , case when history_event_type_code = 'CGU_LossLocation' then regexp_replace(substring(description, '.* to ''(.*)'''), E'[\\n\\r\\u2028]+', ' ', 'g') else NULL end::text as loss_location_amend_to
    , case when description ilike '%at fault changed%' then 'CGU_AtFault' else history_event_type_code end::character varying(50) as history_event_type_code_new
    from 
        ctx.mv_cc_ci_history mcch 
    where public_id not in (select public_id from ctx.mv_cc_ci_claim_history_deprecations)
--  and event_time_stamp>='2017-01-01' --this can be added to remove the inconsistencies in claim desc changes
)
,

cte_hist_event_time_asc as 
(
select *
    , row_number() over (partition by claim_id, history_event_type_code_new order by claim_id, history_event_type_code_new, event_time_stamp) as row_num
    from cte_history
),

cte_claim_first_hist as 
(
    select distinct 
      claim_id as claim_id_hist
    , min(case when history_event_type_code_new = 'CGU_WtHappned' then event_time_stamp end) over (partition by claim_id) as description_first_amend_timestamp
    , min(description_amend_to_only) over (partition by claim_id) as description_first_amend_to
    , min(case when history_event_type_code_new = 'CGU_AtFault' then event_time_stamp end) over (partition by claim_id) as at_fault_first_amend_timestamp
    , min(at_fault_amend_from) over (partition by claim_id) as at_fault_first_amend_from
    , min(at_fault_amend_to) over (partition by claim_id) as at_fault_first_amend_to
    , min(case when history_event_type_code_new = 'CGU_NatureOfLoss' then event_time_stamp end) over (partition by claim_id) as nol_first_amend_timestamp
    , min(nature_of_loss_amend_from) over (partition by claim_id) as nature_of_loss_first_amend_from
    , min(nature_of_loss_amend_to) over (partition by claim_id) as nature_of_loss_first_amend_to
    , min(case when history_event_type_code_new = 'CGU_LossCause' then event_time_stamp end) over (partition by claim_id) as loss_cause_first_amend_timestamp
    , min(loss_cause_amend_from) over (partition by claim_id) as loss_cause_first_amend_from
    , min(loss_cause_amend_to) over (partition by claim_id) as loss_cause_first_amend_to
    , min(case when history_event_type_code_new = 'CGU_DateOfLoss' then event_time_stamp end) over (partition by claim_id) as loss_time_first_amend_timestamp
    , min(claim_loss_time_amend_from) over (partition by claim_id) as claim_loss_time_first_amend_from
    , min(claim_loss_time_amend_to) over (partition by claim_id) as claim_loss_time_first_amend_to
    , min(case when history_event_type_code_new = 'CGU_LossLocation' then event_time_stamp end) over (partition by claim_id) as loss_location_first_amend_timestamp
    , min(loss_location_amend_from) over (partition by claim_id) as loss_location_first_amend_from
    , min(loss_location_amend_to) over (partition by claim_id) as loss_location_first_amend_to
    from 
        cte_hist_event_time_asc
    where 
        row_num=1
        and history_event_type_code_new in ('CGU_WtHappned','CGU_AtFault','CGU_NatureOfLoss','CGU_LossCause','CGU_DateOfLoss','CGU_LossLocation')
),

-- cte_assessing as 
-- (
--     select 
--       substr(exposure_id,3,10)::bigint as assess_exposure_id
--     , reporting_date
--     , report_source
--     , orm_claim_type_name 
--     -- Policy
--     , sum_insured_amount 
--     , sum_insured_type_name 
--     --exp
--     , di_repairerchoice_name
--     , di_repairerchoice_flag
--     , di_repairerchoice_desc
--     -- Vehicle
--     , vehicle_id as assess_vehicle_id
--     , vehicle_rego_number as assess_vehicle_rego_number
--     , vehicle_year 
--     , vehicle_model_desc
--     , vehicle_series_desc
--     , vehicle_make_name
--     , vehicle_make_group
--     , orm_vehicle_other_make_desc
--     , vehicle_body_type_code as assess_vehicle_body_type_code
--     , vehicle_body_type_name as assess_vehicle_body_type_name
--     , vehicle_class_code
--     , vehicle_class_name
--     , vehicle_class_type_code as assess_vehicle_class_type_code
--     , vehicle_class_type_name as assess_vehicle_class_type_name
--     , market_value_amount
--     , prior_to_accident_value_amount
--     , calc_sum_insured_amount
--     , age_group_score
--     , make_score
--     , sum_insured_score
--     , vehicle_score
--     , vehicle_score_band_id
--     , vehicle_score_band_name
--     , vehicle_score_category_id
--     , vehicle_score_category_name
--     -- Towing
--     , orm_towed_with_estimates_flag_code
--     , orm_towed_with_estimates_flag_name
--     , orm_towed_flag_code
--     , orm_towed_flag_name
--     , claim_center_towed_flag_code
--     , claim_center_towed_flag_name
--     , non_trade_tow_flag_code
--     , non_trade_tow_flag_name
--     , trade_tow_flag_code
--     , trade_tow_flag_name
--     , towed_flag_code
--     , towed_flag_name
--     , towed_paid_amount
--     , trade_towed_paid_amount
--     --Salvage
--     , salvage_deduction_amount
--     , salvage_recovery_amount
--     , salvage_amount
--     , calc_salvage_amount
--     --Quote
--     , request_for_quote_id
--     , latest_quote_id
--     , pcm_model_flag_name
--     , quotation_received_date
--     , quotation_approval_date
--     , repairer_name
--     , repairer_code
--     , repairer_name_code
--     , repairer_name_group
--     , repairer_relationship_type_code
--     , repairer_relationship_type_name
--     , repairer_relationship_group_code
--     , repairer_relationship_group_name
--     , repairer_partner_group_name
--     , managed_area_code
--     , managed_area_pod_code
--     , managed_area_pod_name
--     , managed_region_name
--     , assessing_run_code
--     , repairer_post_code
--     , repairer_state
--     , metro_rural_group_name
--     , scorecard_plan_type_name
--     , motor_assessing_kpi_level1
--     , motor_assessing_kpi_level2
--     , motor_assessing_kpi_level3
--     , motor_assessing_kpi_level4
--     , motor_assessing_kpi_level5
--     , motor_assessing_kpi_level6
--     , supplier_partner_flag_name
--     , process_type_name
--     , hub_code
--     , hub_name
--     , original_repairer_code
--     , original_repairer_name
--     --at the time of assessment
--     , as_at_repairer_relationship_type_code
--     , as_at_repairer_relationship_type_name
--     , as_at_repairer_relationship_type_name_group
--     , as_at_repairer_partner_group_name
--     , as_at_scorecard_plan_type_name
--     , as_at_cost_target
--     --Assessment
--     , assessment_date
--     , months_since_assessment
--     , assessor_payroll_number
--     , assessor_name
--     , assessor_name_payroll_desc
--     , assessor_name_group
--     , original_adjustment_method_name
--     , original_repair_cost_amount
--     , original_paint_cost_amount
--     , original_parts_cost_amount
--     , original_removal_cost_amount
--     , original_towing_cost_amount
--     , original_miscellaneous_cost_amount
--     , original_mechanical_cost_amount
--     , original_assessment_cost_inc_excess_exc_gst_amount
--     , latest_adjustment_method_name
--     , latest_repair_cost_amount
--     , latest_paint_cost_amount
--     , latest_parts_cost_amount
--     , latest_removal_cost_amount
--     , latest_towing_cost_amount
--     , latest_miscellaneous_cost_amount
--     , latest_mechanical_cost_amount
--     , latest_assessment_cost_inc_excess_exc_gst_amount
--     , latest_additional_cost_amount
--     , repair_cost_range_name
--     , repair_cost_range_sort_id
--     , repair_cost_range_group_name
--     , repair_cost_range_group_sort_id
--     , assessment_count
--     , additional_assessment_count
--     , additional_assessment_amount
--     , additional_assessment_flag_name
--     , quote_variation_approval_date
--     , quote_variation_received_date
--     , latest_quote_variation_approval_date
--     , latest_quote_variation_received_date
--     , assessment_process_type_name
--     , orm_vehicle_class_name
--     , external_assessor_supplier_code
--     , external_assessor_supplier_name
--     , assesor_type_flag
--     , oem_sourced_unit_count
--     , after_market_sourced_unit_count
--     , exchanged_sourced_unit_count
--     , used_sourced_unit_count
--     , left_orientation_unit_count
--     , left_orientation_count
--     , right_orientation_unit_count
--     , right_orientation_count
--     , other_orientation_unit_count
--     , other_orientation_count
--     , quote_item_parts_amount
--     , quote_item_parts_unit_count
--     , quote_item_parts_count
--     , parts_amount_nds
--     , parts_unit_count_nds
--     , parts_count_nds
--     , vehicle_age
--     , vehicle_age_at_assessment
--     , vehicle_age_range_at_assessment
--     --Settlement
--     , settlement_id
--     , latest_settlement_id
--     , invoice_received_date
--     , total_damage_area_count
--     , multiple_damaged_area_name
--     , damage_keys_as_01_combinations
--     , damage_area_1_code
--     , damage_area_1_name
--     , damage_area_2_code
--     , damage_area_2_name
--     , damage_area_3_code
--     , damage_area_3_name
--     , damage_area_4_code
--     , damage_area_4_name
--     , damage_area_5_code
--     , damage_area_5_name
--     , damage_area_6_code
--     , damage_area_6_name
--     , damage_area_7_code
--     , damage_area_7_name
--     , damage_area_8_code
--     , damage_area_8_name
--     , damage_area_9_code
--     , damage_area_9_name
--     , highest_severity_code
--     , highest_severity_name
--     , assessed_from_location_type_name
--     , impact_rating_score
--     , impact_count
--     , impact_rating_group
--     --Repair
--     , repx_repair_start_date
--     , repx_repair_end_date
--     , orm_repair_start_date
--     , orm_repair_end_date
--     , calc_repair_start_date
--     , calc_repair_end_date
--     , in_repair_days
--     , calc_in_repair_days
--     , repair_in_90_days_count
--     --Paid
--     , registered_for_gst_flag_code
--     , supplier_paid_cost_inc_excess_exc_gst_amount
--     , supplier_paid_cost_exc_excess_exc_gst_amount
--     , total_cost_inc_excess_exc_gst_amount
--     , total_cost_exc_excess_exc_gst_amount
--     , first_repair_payment_date
--     , supplier_payment_approval_date
--     , model_nonmodel_name
--     , repairer_cost_model
--     , repairer_relationship_cost_model
--     , cost_inc_excess_exc_gst_factor
--     , cost_exc_excess_exc_gst_factor
--     , adjusted_claims_count
--     , adjusted_total_cost_inc_excess_exc_gst_amount
--     , adjusted_total_cost_exc_excess_exc_gst_amount
--     , predictive_cost_excluded_flag_code
--     , predictive_cost_excluded_flag_name
--     , total_cost_inc_excess_exc_gst_net_cal_salvage_amount
--     , adjusted_total_cost_inc_excess_exc_gst_net_cal_salvage_amount
--     , total_cost_inc_excess_exc_gst_net_cal_salvage_net_towed_amount
--     , adj_total_cost_inc_excess_exc_gst_net_cal_salvage_net_towed_amt
--     --Parts
--     , oem_sourced_amount
--     , after_market_sourced_amount
--     , exchanged_sourced_amount
--     , used_sourced_amount
--     , oem_sourced_count
--     , after_market_sourced_count
--     , exchanged_sourced_count
--     , used_sourced_count
--     , left_orientation_amount
--     , right_orientation_amount
--     , other_orientation_amount
--     --Reporting flag
--     , report_valid_flag_code
--     , claims_count_factor
--     --Original quote
--     , original_quote_received_cost_amount
--     , original_quote_vehicle_rego_number
--     , job_type_date
--     , job_type_name
--     --Customer hub
--     , customer_hub_id
--     , customer_hub_name
--     , customer_hub_status
--     , repairer_status
--     , customer_hub_repair_start_date
--     , customer_hub_repair_end_date
--     , customer_hub_flag
--     , reallocation_repair_hub_scope_flag_code
--     , reallocation_repair_hub_scope_flag_name
--     , reallocation_reason
--     from pub.mv_motor_assessing_monthly_historical 
--     where month_to_date_reporting_flag = 'Y'
--     and datasource='CC_CI'
-- ),

cte_claim_alert as 
(
    select *
    ,greatest(faa_triage_flag, manual_triage_flag, investigation_flag) as triage_flag 
    from 
    (
        select 
        claim_number as alert_claim_number   
        , faa_alert_flag
        , auto_alert_flag
        , manual_referral_flag 
        , case when faa_disposition_category in ('Rejected', 'Retained') then 1 else 0 end as faa_triage_flag
        , case when faa_disposition_category in ('Retained') then 1 else 0 end as faa_retained_triage_flag
        , case when referral_category IN ('CC Manual','CTP Internal Only', 'CC Manual & FAA') then 1 else 0 end as manual_triage_flag
        , case when accepted_flag = 1 or denied_withdrawn_flag = 1 then claim_number else null end as investigation_claim_number
        , case when fraud_risk_status_name is not null OR invest_accepted_flag =1 then 1 else 0 end as investigation_flag --this means that there is an investigation outcome
        , case when fraud_risk_status_name = 'Alleged' then 1 else 0 end as denied_outcome_flag
        , case when referral_category='CTP Internal Only' then 1 else 0 end as ctp_internal_flag
        , case when referral_category IN ('CC Manual','CTP Internal Only', 'CC Manual & FAA') 
        			OR faa_disposition_category ='Retained' 
        			OR fraud_risk_status_name is not null 
                    OR invest_accepted_flag =1
        			then 1 else 0 end as fraud_referral_flag
        , denied_withdrawn_flag
        , accepted_flag
        , investigation_savings as investigation_savings
        , initial_activity_time
        FROM pub.mv_fraud_investigations_cc_ci_ctp
--	    WHERE
--           claim_lodgement_date::date BETWEEN  '{{ var("claim_start_date") }}'  AND '{{ var("claim_end_date") }}'    
    ) fili
)
,

cte_inv_cost as
(
    select trans.claim_number as inv_cost_claim_number
    , sum(trans.gross_paid_inc_gst) as inv_cost
    from pub.mv_claim_transactions_harmonised trans
    left join pub.mv_sf_ec_employee emp 
        on trans.transaction_creator_payroll = lower(emp.user_id) 
         and transaction_date between emp.derived_start_date and emp.derived_end_date
    left join bus_ref_umt.fraud_investigation_mapping map 
        on trans.payee_type_name = map.payee_type_name
    where trans.transaction_type_name = 'Issued Payment' 
--        and trans.transaction_date >='2019-07-01' 
        and (lower(emp.department_name) ~~ ANY(array['%fraud & investigations','%frdia & investigations'])
        or (lower(trans.transaction_create_consultant) ~~ ANY(array['esbservice esbservice','system user','%system','super user','sys_claims auto']) 
            AND map.category is not null))      
    group by 1
)
,

cte_tran as 
(
    /* this is at claim level */
    select claim_number as tran_claim_number
    , sum(first_expr_open_reserves) as first_open_reserves
    , sum(expr_gross_paid) as gross_paid
    from 
    (
        select distinct claim_number
        , claim_exposure_id
        , first_value(case when transaction_source = 'RESERVE' and open_reserves_inc_gst>0 then open_reserves_inc_gst end) over (partition by claim_number, claim_exposure_id order by transaction_created_date, open_reserves_inc_gst desc) as first_expr_open_reserves
        , sum(case when transaction_source = 'PAYMENT' then gross_paid_inc_gst end) over (partition by claim_number, claim_exposure_id) as expr_gross_paid
        from pub.mv_claim_transactions_harmonised 
        where source_system = 'CC_CI'
    --    and
    --        claim_lodgement_date::date BETWEEN  '{{ var("claim_start_date") }}'  AND '{{ var("claim_end_date") }}' 
    ) transactions
    group by 1
)
,

cte_address as 
(
select 
      address_id
    , gnaf 
    , address_text 
    , latitude 
    , longitude 
    , confidence 
    , reliability 
    , date_created 
    from 
        ctx.mv_cc_ci_address_geocode_enriched mccage 
),


    
claim_aggregate as (
select 
  cte_claim.*
, cte_claim_time.* 
, cte_incident.*
, cte_vehicle.*
, cte_exposure.*  
, cte_riskunit.*
-- , cte_assessing.*
, case when cte_claim_first_hist.at_fault_first_amend_from = 'Blank' then cte_claim_first_hist.at_fault_first_amend_to
       when cte_claim_first_hist.at_fault_first_amend_from is not null then cte_claim_first_hist.at_fault_first_amend_from
       when cte_claim_first_hist.at_fault_first_amend_from is null and cte_claim.fault_rating_name not ilike '%third party%' then cte_claim.fault_rating_name
       when cte_claim_first_hist.at_fault_first_amend_from is null and cte_claim.fault_rating_name ilike '%third party%' then 'Third Party'
  end::text as fault_rating_at_lodgement
, cte_claim_first_hist.at_fault_first_amend_timestamp
, coalesce(cte_claim_first_hist.nature_of_loss_first_amend_from, cte_claim.general_nature_of_loss_name) as general_nature_of_loss_at_lodgement
, cte_claim_first_hist.nol_first_amend_timestamp
, coalesce(cte_claim_first_hist.loss_cause_first_amend_from, cte_claim.cause_of_loss_name) as cause_of_loss_at_lodgement
, cte_claim_first_hist.loss_cause_first_amend_timestamp
, coalesce(cte_claim_first_hist.claim_loss_time_first_amend_from, cte_claim_time.claim_loss_time) as claim_loss_time_at_lodgement
, cte_claim_first_hist.loss_time_first_amend_timestamp
, cte_claim_first_hist.loss_location_first_amend_timestamp
, coalesce(cte_claim_first_hist.description_first_amend_to, cte_claim.claim_description) as claim_description_close_to_lodgement --there is no amend from for claim desc
, cte_claim_first_hist.description_first_amend_timestamp
, cte_claim_alert.*
, cte_inv_cost.*
, cte_tran.*
, cte_address.*
--  end::text as collision_type_at_lodgement
, now() as created_timestamp
, roles_flag.name_involved_party
, roles_flag.role_witness_flag
, roles_flag.insured_is_driver_flag
, roles_flag.driver_is_insured_spouse_flag
, roles_flag.pedestrian_is_injured_flag
, roles_flag.witness_has_family_relationship_flag

from 
    cte_claim 
left join cte_claim_time
    on cte_claim_time.claim_number2 = cte_claim.claim_number
join cte_incident
    on cte_claim.claim_id = cte_incident.incident_claim_id
left join cte_vehicle
    on cte_vehicle.vehicle_id = cte_incident.incident_vehicle_id
left join cte_exposure --use left join if you want information on other vehicles that do not have exposures
    on cte_exposure.expr_incident_id = cte_incident.incident_id
left join ctx.mv_cc_ci_coverage covr 
    on covr.coverage_id = cte_exposure.coverage_id 
       and covr.retired = 0
left join cte_riskunit 
    on cte_riskunit.risk_unit_id = covr.risk_unit_id 
left join cte_claim_first_hist
    on cte_claim.claim_id=cte_claim_first_hist.claim_id_hist
left join cte_claim_alert
    on cte_claim_alert.alert_claim_number=cte_claim.claim_number
left join cte_inv_cost
    on cte_inv_cost.inv_cost_claim_number=cte_claim.claim_number
left join cte_tran
    on cte_tran.tran_claim_number=cte_claim.claim_number
 left join cte_address
     on cte_address.address_id=cte_claim.loss_location_id
left join roles_flag
    on roles_flag.claim_number=cte_claim.claim_number
-- left join cte_assessing
    -- on cte_assessing.assess_exposure_id = cte_exposure.exposure_id
)

select 
        claim_number,
        policy_number,
        claim_id,
        policy_brand,
        MAX(policy_system_type_code) AS policy_system_type_code,
        MAX(claim_lodgement_date) AS claim_lodgement_date,
        MAX(claim_report_date) AS claim_report_date,
        MAX(claim_loss_date) AS claim_loss_date,
        MAX(claim_loss_time_at_lodgement) AS claim_loss_time_at_lodgement,
        MAX(police_flag) AS police_involved_flag,
        MAX (police_reported_date) AS police_reported_date,
        MAX(policy_risk_number) AS policy_risk_number,
        MAX(claim_loss_time) AS claim_loss_time,
        MAX(claim_how_reported_name) AS claim_how_reported_name,
        MAX(loss_suburb_name) AS loss_suburb_name,
        MAX(claim_postcode) AS claim_postcode,
        MAX(claim_loss_state) AS claim_loss_state,
        MAX(claim_description) AS claim_description,
        MAX(claim_description_close_to_lodgement) AS claim_description_close_to_lodgement,
        MAX(general_nature_of_loss_name) AS general_nature_of_loss_name,
        MAX(general_nature_of_loss_at_lodgement) AS general_nature_of_loss_at_lodgement,
        MAX(cause_of_loss_name) AS cause_of_loss_name,
        MAX(cause_of_loss_at_lodgement) AS cause_of_loss_at_lodgement,
        MAX(fault_rating_name) AS fault_rating_name,
        MAX(fault_rating_at_lodgement) AS fault_rating_at_lodgement,
        MAX(latitude) AS latitude,
        MAX(longitude) AS longitude,
        MAX(gnaf) AS gnaf,
        MAX(claim_closed_outcome_name) AS claim_closed_outcome_name,
        MAX(claim_status_name) AS claim_status_name,
        MAX(claim_status_conformed) AS claim_status_conformed,
        MAX(faa_alert_flag) AS faa_alert_flag,
        MAX(faa_triage_flag) AS faa_triage_flag,
        MAX(faa_retained_triage_flag) AS faa_retained_triage_flag,
        MAX(manual_referral_flag) AS manual_referral_flag,
        MAX(triage_flag) AS triage_flag,
        MAX(manual_triage_flag) as manual_triage_flag,
        MAX(investigation_flag) AS investigation_flag,
        MAX(investigation_savings) AS investigation_savings,
        MAX(denied_outcome_flag) AS denied_outcome_flag,
        MAX(ctp_internal_flag) AS  ctp_internal_only_flag,     
        MAX(denied_withdrawn_flag) AS denied_withdrawn_flag,
        MAX(fraud_referral_flag) AS fraud_referral_flag,
        -- MAX(rni_flag) AS rni_flag,
        MAX(role_witness_flag) AS witness_flag,
        SUM(role_witness_flag) AS witness_amt,
        MAX(insured_is_driver_flag) AS insured_is_driver_flag,
        MAX(driver_is_insured_spouse_flag) AS driver_is_insured_spouse_flag,
        MAX(pedestrian_is_injured_flag) AS pedestrian_is_injured_flag,
        MAX(witness_has_family_relationship_flag) AS witness_has_family_relationship_flag,
        COUNT(DISTINCT incident_vehicle_id) AS vehicle_count_at_lodgement
from claim_aggregate
where investigation_flag is not null
      and triage_flag is not null
--       and denied_withdrawn_flag is not null
group by claim_number,
         policy_number,
         claim_id,
         policy_brand