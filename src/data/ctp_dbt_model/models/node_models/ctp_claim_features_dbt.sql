
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                               
-- Description: This table is used to extract lodgement and liability
-- features for CTP claims
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     09/04/2024   Xiaomin Chang             Initial release        
-- -------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['claim_number'],
          post_hook = grant_access(this)
        ) }}


WITH cte_ctp_claim AS
(	
	SELECT  claim_number,
			policy_number,
			claim_id,
			claim_lodgement_date,
			claim_loss_date,
			claim_status_name,
			claim_report_date,
		        (CAST(claim_lodgement_date AS date)- CAST (claim_loss_date AS date)) AS late_lodgement_days,
		        (CAST(claim_report_date AS date)- CAST (claim_loss_date AS date)) AS late_report_days,
			claim_how_reported_name,
			claim_description_close_to_lodgement,
			cause_of_loss_name,
			police_involved_flag,
			(CAST(police_reported_date AS date)- CAST (claim_loss_date AS date)) AS late_report_police_days,
			faa_alert_flag,
			faa_triage_flag,
			faa_retained_triage_flag,
			manual_referral_flag,
			triage_flag,
			manual_triage_flag,
			investigation_flag,
			denied_outcome_flag,
			ctp_internal_only_flag,     
			denied_withdrawn_flag,
			fraud_referral_flag,
			witness_flag,
			witness_amt,
			insured_is_driver_flag,
			driver_is_insured_spouse_flag,
			pedestrian_is_injured_flag,
			witness_has_family_relationship_flag,
			investigation_savings,
			vehicle_count_at_lodgement
	FROM {{ ref('ctp_claims_data_dbt') }}
),

-- no capacity for work > 1 month with minor injuries
cte_injury_info as
(
	select mccs.claim_number, 
		   mccs.exposure_id,
		   max(case when mccs.was_not_fit_for_work_flag ='Y' then 1 else 0 end) as not_fit_for_work_flag,
		   max(case when mccs.estimate_of_injury_severity_name like '%Minor%' and mccs.was_not_fit_for_work_flag ='Y' then 1 else 0 end) as off_work_minor_injury_flag,
		   max(case when mccs.estimate_of_injury_severity_name like '%Minor%' and mccs.was_not_fit_for_work_flag ='Y' 
		       and mccs.recovery_plan_end_date:: DATE > mccs.recovery_plan_start_date:: DATE + INTERVAL '1 month' then 1 else 0 end) as off_work_1_month_minor_injury_flag,
		   max(case when mccs.estimate_of_injury_severity_name like '%Minor%' and mccs.was_not_fit_for_work_flag ='Y'
		   	   then mccs.recovery_plan_end_date:: DATE - mccs.recovery_plan_start_date:: DATE else 0 end) as off_work_minor_injury_days,
		   max(case when mccs.claim_late_name = 'Yes' then 1 else 0 end ) late_claim_flag
	from pub.mv_ctp_claim_summary mccs
	group by
			mccs.claim_number,
			mccs.exposure_id	
),
cte_offwork_summary as
(
	select 
		claim_number,
		max(not_fit_for_work_flag) as not_fit_for_work_flag,
		max(off_work_minor_injury_flag) as off_work_minor_injury_flag,
		sum(off_work_1_month_minor_injury_flag) as offwork_1_month_minor_injury_count,
		max(off_work_minor_injury_days) as max_offwork_minor_injury_days,
		sum(late_claim_flag) AS late_claim_amt,
		max (late_claim_flag) AS late_claim_flag
	from cte_injury_info
	group by claim_number
),

-- property damage claim has been flagged as suspicious.
cte_motor_claim AS (
  SELECT DISTINCT 
	a.claim_number,
	a.claim_loss_date,
	a.claim_id,
	b.vehicle_id,
	d.vehicle_vin,
	MAX(CAST(a.claim_lodgement_date AS date)- CAST (a.claim_loss_date AS date)) AS motor_late_lodgement_days,
	MAX(CAST(a.reported_date AS date)- CAST (a.claim_loss_date AS date)) AS motor_late_report_days,
	MAX(COALESCE(b.does_the_vehicle_require_a_tow::int,0)) AS vehicle_tow_flag,
	MAX(COALESCE(b.vehicle_stolen_flag_id::int, 0)) AS vehicle_stolen_flag,
	MAX(CASE WHEN c.accepted_flag=1 OR c.denied_withdrawn_flag=1 OR c.faa_alert_flag =1 
	THEN 1 ELSE 0 END )AS suspacious_flag
  FROM  
    pub_core.mv_claim_header a
  INNER JOIN ctx.mv_cc_ci_incident b
  ON a.claim_id = b.claim_id
  INNER JOIN pub.mv_fraud_investigations_summarised c
  ON a.claim_number= c.claim_number
  INNER JOIN ctx.mv_cc_ci_incident_driver_vehicle_ext d
  ON d.vehicle_id = b.vehicle_id
  AND d.claim_number=a.claim_number
  WHERE b.vehicle_id IS NOT NULL 
  AND a.claim_loss_type_name = 'Motor' 
  AND a.policy_issue_state='NSW' 
  AND a.notify_only_claim_flag = 'No'
  AND a.claim_closed_outcome_name not in ('Duplicate' ,'Open in error')
  GROUP BY 
		  	a.claim_number,
			a.claim_loss_date,
			a.claim_id,
			b.vehicle_id,
			d.vehicle_vin
),

cte_suspicious_motor AS (
	SELECT 
		 a.claim_number,
		 MAX(d.suspacious_flag) AS suspacious_vehicle_flag,
		 SUM(d.suspacious_flag) AS suspacious_vehicle_amt,
		 MAX(d.vehicle_tow_flag) AS vehicle_tow_flag,
		 MAX (d.vehicle_stolen_flag) AS vehicle_stolen_flag,
		 SUM(d.vehicle_stolen_flag) AS vehicle_stolen_amt,
		 MAX(d.motor_late_report_days) AS max_motor_late_report_days,
		 MAX(d.motor_late_lodgement_days) AS max_motor_late_lodgement_days
	FROM cte_ctp_claim a
	INNER JOIN ctx.mv_cc_ci_incident b
	ON a.claim_id = b.claim_id
	INNER JOIN ctx.mv_cc_ci_incident_driver_vehicle_ext c
	ON b.vehicle_id = c.vehicle_id
	AND c.claim_number=a.claim_number
	INNER JOIN cte_motor_claim d
	ON d.vehicle_vin= c.vehicle_vin
	AND a.claim_loss_date = d.claim_loss_date
	GROUP BY a.claim_number
	),
	
-- Our insured is evasive or cannot be contacted
cte_evasive_contact as(
select 
		a.claim_number, 
		b.contact_prohibited_flag_id, 
		b.contact_prohibited_by_sira_flag,
		b.contact_mobile_number,
		b.email_address_1 ,
		b.email_address_2,
		case when  (b.contact_mobile_number is null 
					and b.email_address_1 is null 
					and b.email_address_2 is null)
					or b.contact_prohibited_flag_id = 'Yes'
					or b.contact_prohibited_by_sira_flag = 'Yes'
		then 1 else 0 end as evasive_flag
from ctx.mv_cc_ci_claim_header_ext a
left join ctx.mv_cc_ci_claim_contact_ext b
on a.claim_number = b.claim_number 
where a.claim_loss_type_name = 'CTP' 
and a.policy_issue_state='NSW' 
and a.notify_only_claim_flag = 'No'
and b.role_name='Insured'
),

cte_evasive_indicator AS (
select claim_number,
	   max(evasive_flag) AS driver_evasive_flag
from cte_evasive_contact
group by claim_number
),

cte_fitness_certificate AS (
	SELECT 
		 a.claim_number,
		 b.claim_exposure_id,
		 MAX(b.fitness_certificate_start_date - a.claim_loss_date) AS max_late_treatment_days,
		 COALESCE(COUNT(DISTINCT b.fitness_certificate_id), 0) AS amt_fitness_certificate
	from 
		cte_ctp_claim a
	INNER JOIN pub_core.claim_exposure_weekly_benefit_summary b
	ON a.claim_number= b.claim_number
	GROUP BY 
				a.claim_number,
		 		b.claim_exposure_id
),


cte_fitness_certificate_summary AS (
SELECT claim_number,
	   MAX(max_late_treatment_days) AS max_late_treatment_days,
	   MAX(amt_fitness_certificate) AS max_amt_fitness_certificate_per_exp
FROM cte_fitness_certificate
GROUP BY claim_number
),



-- delay_complaint_after_claim as (
-- SELECT  
-- 	 a.claim_number,
-- 	 MIN(b.complaint_received_date - a.claim_loss_date) AS late_complaint_received_days
-- FROM  cte_ctp_claim a
-- INNER JOIN  pub_restricted.mv_svx_iag_customer_activity_complaints b
-- 	on b.complaint_claim_number = a.claim_number 
-- GROUP BY  a.claim_number 
-- ),

injured_employment_info as (
SELECT 	   claim_number,
	   MAX(CASE WHEN work_capacity in ('11 - Not Working - Student (aged 15 or over)', '10 - Not Working - Child (aged 14 or under)') THEN 1 ELSE 0 end) as child_student_injured_flag,
	   MAX(CASE WHEN work_capacity in ('26 - Not working - Has Work capacity','06 - Not Working') or pre_accident_employment_status ilike '%17 - Not Working - receiving weekly payments from a previous accident%' THEN 1 ELSE 0 end) AS suspacious_not_working_flag,
	   MAX(CASE WHEN work_capacity ILIKE '%New Employer%' THEN 1 ELSE 0 end) AS recently_employed_flag,
	   MAX(CASE WHEN work_capacity ILIKE '%Self Employed%' THEN 1 ELSE 0 end) AS self_employed_flag,
	   MAX(COALESCE (pre_accident_total_average_weekly_earning,0)) AS max_pre_accident_weekly_earning
FROM pub.mv_ctp_claim_summary
GROUP BY claim_number 
),

motor_assess_info AS (  
SELECT a.claim_number, 
	b.claim_loss_date,
	vehicle_rego_number,
	highest_severity_code,
	highest_severity_name
FROM pub.mv_motor_assessing a 
INNER JOIN ctx.mv_cc_ci_claim_header_ext b
ON  a.claim_number=b.claim_number
),


motor_injury_assess AS (
SELECT a.claim_number, a.vehicle_rego_number, a.claim_exposure_id,
max(CASE WHEN a.estimate_of_injury_severity_code <> '1' AND b.highest_severity_code IN ('LP','HP') 
	THEN 1 ELSE 0 END) AS accident_injury_mismatch_flag,
max(b.highest_severity_name) AS motor_accident_severity, 
max(a.estimate_of_injury_severity_name) AS injury_severity
FROM pub.mv_ctp_claim a
LEFT JOIN motor_assess_info b
ON a.claim_loss_date = b.claim_loss_date
AND a.vehicle_rego_number = b.vehicle_rego_number
WHERE ctp_statutory_insurer_state_code='AU_NSW'
  AND claim_status_name <> 'Draft' 
  AND claim_closed_outcome_name not in ('Duplicate' ,'Open in error' ,'Cancelled')
GROUP BY a.claim_number, a.claim_exposure_id, a.vehicle_rego_number 
),

motor_injury_summary AS (
SELECT  claim_number, 
	MAX(accident_injury_mismatch_flag) AS accident_injury_mismatch_flag
FROM motor_injury_assess
GROUP BY claim_number
)


	
SELECT  a.claim_number,
        a.policy_number,
        a.claim_id,
        a.claim_lodgement_date,
        a.claim_loss_date,
        a.claim_report_date,
        a.late_lodgement_days,
        a.late_report_days,
        a.claim_how_reported_name,
	a.claim_status_name,
        a.cause_of_loss_name,
	a.police_involved_flag,
	a.late_report_police_days,
	a.faa_triage_flag,
	a.faa_retained_triage_flag,
	a.manual_triage_flag,
        a.triage_flag,
        a.investigation_flag,
        a.denied_withdrawn_flag,
	a.denied_outcome_flag,
	a.ctp_internal_only_flag,
	a.fraud_referral_flag,
	a.investigation_savings,
	a.faa_alert_flag,
        a.manual_referral_flag,
        a.witness_flag,
        a.witness_amt,
        a.insured_is_driver_flag,
        a.driver_is_insured_spouse_flag,
        a.pedestrian_is_injured_flag,
        a.witness_has_family_relationship_flag,
        a.vehicle_count_at_lodgement,
	b.not_fit_for_work_flag,
	b.off_work_minor_injury_flag,
        b.offwork_1_month_minor_injury_count,
        b.max_offwork_minor_injury_days,
        b.late_claim_amt,
        b.late_claim_flag,
        c.suspacious_vehicle_flag,
        c.suspacious_vehicle_amt,
        c.vehicle_tow_flag,
        c.vehicle_stolen_flag,
        c.vehicle_stolen_amt,  
        c.max_motor_late_report_days,
        c.max_motor_late_lodgement_days,
        d.driver_evasive_flag,
        e.max_late_treatment_days,
        e.max_amt_fitness_certificate_per_exp,
        -- f.late_complaint_received_days,
	g.recently_employed_flag,
	g.self_employed_flag,
	g.max_pre_accident_weekly_earning,
	g.child_student_injured_flag,
	g.suspacious_not_working_flag,
	h.accident_injury_mismatch_flag

FROM cte_ctp_claim a
LEFT JOIN cte_offwork_summary b
ON a.claim_number =b.claim_number
LEFT JOIN cte_suspicious_motor c
ON a.claim_number =c.claim_number
LEFT JOIN cte_evasive_indicator d
ON a.claim_number =d.claim_number
LEFT JOIN cte_fitness_certificate_summary e
ON a.claim_number =e.claim_number
-- LEFT JOIN delay_complaint_after_claim f
-- ON a.claim_number =f.claim_number
LEFT JOIN injured_employment_info g
on a.claim_number = g.claim_number
LEFT JOIN motor_injury_summary h
on a.claim_number = h.claim_number
