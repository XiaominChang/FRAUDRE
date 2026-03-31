-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                              
-- Description: Features related to medical conditions and injuries for CTP modelling
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     16/03/2024   Xiaomin Chang             Initial release   
-- 2.00     25/03/2024   Xiaomin Chang             V2          
-- -------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['claim_number'],
		  post_hook = grant_access(this)
          ) }}


WITH cte_medical_info AS (
	SELECT 
		a.claim_number,
		a.injured_person,
		max(a.hospitalisation_status_code),
		max(a.admitted_to_hospital_code),
		max(a.injury_coding_status_name),
		max(a.estimate_of_injury_severity_name),
		max(a.date_of_injury_severity_estimate),
		max(a.minor_injury_decision_name),
		max(a.reason_for_minor_injury_decision_name),
		max(a.minor_injury_assessment_type_name),
		max(a.date_of_minor_injury_assessment_decision),
		max(a.outcome_of_wpi_assessment_name),
		max(a.final_wpi_percentage),
		max(a.medical_treatment_type_name),
		max(a.disabled_due_to_accident_name),
		max(CASE WHEN estimate_of_injury_severity_code='1' THEN 1 ELSE 0 END) AS minor_injury_flag,
		max(CASE WHEN a.risk_screening_outcome_name IN ('1 - Poor risk recovery','2 - Medium risk recovery','High', 'Medium')
			THEN 1 ELSE 0 END) AS high_risk_flag,
		max(CASE WHEN minor_injury_decision_code = '0' THEN 1 ELSE 0 END) AS not_threshold_injury_flag,
		max(CASE WHEN days_of_hospital_stay_following_the_accident IS NOT NULL 
					  OR hospitalisation_status_code IN ('02','03')
		THEN 1 ELSE 0 END) AS go_hospital_after_accident_flag,
		max(CASE WHEN treated_at_hospital_flag='Yes' THEN 1 ELSE 0 END) AS treated_at_hospital_flag,
		max(CASE WHEN injury_coding_status_code ='4' THEN 1 ELSE 0 END) AS injury_coding_finalised_flag,
		max(CASE WHEN go_hospital_after_accident_flag='Yes'
			THEN CAST(COALESCE(admission_date, first_medical_exam_date) AS date) - CAST (b.claim_loss_date AS date) 
			ELSE 0 END) AS days_of_delay_to_hospital,
		max(CASE WHEN minor_injury_assessment_type_name IS NOT NULL
			THEN CAST(date_of_minor_injury_assessment_decision AS date) - CAST (b.claim_lodgement_date AS date) 
			ELSE 0 END) AS days_of_minor_assess_to_lodgement,			
		max(COALESCE(CAST(days_of_hospital_stay_following_the_accident AS INT), 0)) AS days_of_hospital_stay_following_the_accident,
		max(CASE WHEN a.injury_severity_name ='Death' THEN 1 ELSE 0 END) AS fatal_flag,
		max(CASE WHEN prior_injury_disability = 'Yes' THEN 1 ELSE 0 END) AS prior_disability_flag,
		max(CASE WHEN detailed_injury_type_name ='125 - Psychological' 
				OR (minor_injury_decision_name IN ('4 - Threshold Injury - Psych only', '5 - Threshold Injury - Both soft tissue and psych')) 
			THEN 1 ELSE 0 END) AS psych_flag,
		max(CASE WHEN detailed_injury_type_name ='125 - Psychological' 
				AND minor_injury_decision_name =' 0 - Not Threshold Injury'
			THEN 1 ELSE 0 
			END) AS not_threshold_psych_injury_flag,
		max(CASE WHEN detailed_injury_type_name ='125 - Psychological'
				OR detailed_injury_type_name ='Unknown' 
				OR detailed_injury_type_name IS NULL 
			THEN 1 ELSE 0 
			END) AS non_demonstrable_injury_flag,
		max(CASE WHEN hospitalisation_status_code= '01' THEN 1 ELSE 0 END) AS not_attend_hospital_immediately_flag,
		max(CASE WHEN role_of_ambulance_code IN ('02','03','04') THEN 1 ELSE 0 END) AS ambulance_attendance_flag,
		max(CASE WHEN ambulance_required_name= 'Yes' THEN 1 ELSE 0 END) AS ambulance_required_flag,
		
		max(CASE WHEN hospitalisation_status_code= '01' AND estimate_of_injury_severity_code::INT>2 
			THEN 1 ELSE 0 END) AS serious_injury_not_attend_hospital_imm_flag,
			
		max(CASE WHEN hospitalisation_status_code IN ( '02', '03') AND estimate_of_injury_severity_code='1'
			THEN 1 ELSE 0 END) AS minor_injury_attend_hospital_flag,
		
		max(CASE WHEN role_of_ambulance_code ='01' AND estimate_of_injury_severity_code::INT> 2 
			THEN 1 ELSE 0 END) AS serious_injury_no_ambulance_flag,
		
		max(CASE WHEN role_of_ambulance_code ='04' AND estimate_of_injury_severity_code='1'
			THEN 1 ELSE 0 END) AS minor_injury_with_ambulance_transport_flag,
		
		max(CASE WHEN hospitalisation_status_code = '01'
					AND role_of_ambulance_code IN ( '02','03' )
			THEN 1 ELSE 0 
			END) AS ambulance_without_hospitalisation_flag,
		max(CASE WHEN hospitalised_over_24_hours_name='Yes'	
			THEN 1 ELSE 0 
			END) AS hospitalised_over_24_hours_flag,	
		max(CASE WHEN (go_hospital_after_accident_flag = 'No' OR treated_at_hospital_flag='No')
						AND days_of_hospital_stay_following_the_accident IS NOT NULL 
			THEN 1 ELSE 0 
			END) AS hospital_conflict_flag,
		max(CASE WHEN disabled_due_to_accident_name IN ('Totally Disabled','Partially Disabled') 
			THEN 1 ELSE 0
			END) AS disabled_due_to_accident_flag,
		max(CASE WHEN helmet_worn_code ='No' THEN 1 ELSE 0 END) AS no_helment_worn_flag,
		max(CASE WHEN seat_beat_fastened_code ='No' THEN 1 ELSE 0 END) AS seatbelt_unfastened_flag,
		max(CASE WHEN alcohol_consumed_code='Yes' THEN 1 ELSE 0 END) AS alcohol_consumed_flag,		
		b.policy_number,
		b.claim_loss_date,
		b.claim_lodgement_date
--		b.cause_of_loss_name,
--		b.claim_insured_contact_name 
	FROM pub.ctp_injury a
	INNER JOIN pub_core.mv_claim_header b
	ON a.claim_number = b.claim_number 
	WHERE b.policy_issue_state='NSW'
	GROUP BY a.claim_number,
		 b.policy_number,
		 b.claim_lodgement_date,
		 b.claim_loss_date,
		 a.injured_person
),

cte_medical_summery AS (
	SELECT 
		claim_number,
		policy_number,
		claim_lodgement_date,
		COUNT(DISTINCT injured_person) as injured_person_amt,
		MAX(days_of_delay_to_hospital) AS max_days_of_delay_to_hospital,
		MAX(days_of_minor_assess_to_lodgement) AS max_days_of_minor_assess_to_lodgement,
		MAX(days_of_hospital_stay_following_the_accident) AS max_days_of_hospital_stay_following_the_accident,
		SUM(go_hospital_after_accident_flag) AS go_hospital_after_accident_amt,
		SUM(high_risk_flag) AS high_risk_amt,
		SUM(high_risk_flag)::FLOAT /NULLIF(count(DISTINCT injured_person),0) AS proportion_of_high_risk,
		SUM(minor_injury_flag) AS minor_injury_amt,
		SUM(minor_injury_flag)::FLOAT /NULLIF(count(DISTINCT injured_person),0) AS proportion_of_minor_injury,
		SUM(not_threshold_injury_flag) AS not_threshold_injury_amt,
		SUM(not_threshold_injury_flag)::FLOAT/NULLIF(count(DISTINCT injured_person),0) AS proportion_of_not_threshold_injury,
		SUM(treated_at_hospital_flag) AS treated_at_hospital_amt,
		SUM(injury_coding_finalised_flag)::FLOAT/NULLIF(count(DISTINCT injured_person),0) AS proportion_of_coding_finalised,
		SUM(fatal_flag) AS fatal_amt,
		SUM(prior_disability_flag) AS prior_disability_amt,
		SUM(psych_flag) AS psycho_injury_amt,
		SUM(psych_flag)::FLOAT /NULLIF(count(DISTINCT injured_person),0) AS proportion_of_psych_injury,
		SUM(not_threshold_psych_injury_flag) AS not_threshold_psych_injury_amt,
		SUM(not_threshold_psych_injury_flag)::FLOAT /NULLIF(count(DISTINCT injured_person),0) AS proportion_of_not_threshold_psych_injury,
		SUM(non_demonstrable_injury_flag) AS non_demonstrable_injury_flag_amt,
		SUM(not_attend_hospital_immediately_flag) AS not_attend_hospital_immediately_amt,
		SUM(serious_injury_not_attend_hospital_imm_flag) as serious_injury_not_attend_hospital_imm_amt,			
		SUM(minor_injury_attend_hospital_flag) as minor_injury_attend_hospital_amt,		
		SUM(serious_injury_no_ambulance_flag) as serious_injury_no_ambulance_amt,	
		SUM(minor_injury_with_ambulance_transport_flag) as minor_injury_with_ambulance_transport_amt,
		SUM(ambulance_attendance_flag) AS ambulance_attendance_amt,
		MAX(ambulance_attendance_flag) AS ambulance_attendance_flag,
		SUM(ambulance_required_flag) AS ambulance_required_amt,
		SUM(ambulance_without_hospitalisation_flag) AS ambulance_without_hospitalisation_amt,
		SUM(hospitalised_over_24_hours_flag) AS hospitalised_over_24_hours_amt,
		SUM(hospital_conflict_flag) AS hospital_conflict_amt,
		SUM(disabled_due_to_accident_flag) AS disabled_due_to_accident_amt,
		SUM(no_helment_worn_flag) AS no_helment_worn_amt,
		SUM(seatbelt_unfastened_flag) AS seatbelt_unfastened_amt,
		SUM(alcohol_consumed_flag) AS alcohol_consumed_amt
	FROM cte_medical_info
	GROUP BY claim_number,
		 policy_number,
		 claim_lodgement_date
),


injured_detailed_info as(
	SELECT  
		a.claim_number,  
		a.policy_number,
		a.claim_lodgement_date,
	        b.contact_id,
	        MAX (b.contact_full_name) AS name_ctp,
	        MAX(CASE WHEN a.claim_insured_contact_name = b.contact_full_name
	        AND b.role_name='Injured Party' 
	        THEN 1 ELSE 0
	        END) AS insured_injured_flag,
	       	MAX(CASE WHEN a.claim_insured_contact_name = b.contact_full_name
	        AND b.role_name='Motor Vehicle Driver' 
	        THEN 1 ELSE 0
	        END) AS insured_driver_flag,
	       	MAX(CASE WHEN a.claim_insured_contact_name = b.contact_full_name
	        AND b.role_name='Passenger' 
	        THEN 1 ELSE 0
	        END) AS insured_passenger_flag,
	        MAX(CASE WHEN b.role_name='Injured Party' 
	        THEN 1 ELSE 0
	        END) AS injured_flag,
	       	MAX(CASE WHEN a.fault_rating_name_conformed='Insured at fault'
	        THEN 1 ELSE 0
	        END) AS insured_at_fault_flag	        
	FROM pub_core.mv_claim_header a
	INNER JOIN  ctx.mv_cc_ci_claim_contact_ext b
	ON a.claim_number = b.claim_number 
	WHERE    
	        a.line_of_business_name_conformed='Compulsory Third Party'
	        AND a.policy_issue_state='NSW' 
	        AND a.notify_only_claim_flag = 'No'
	        AND b.role_name IN ('Motor Vehicle Passenger','Injured Party',
						        'Passenger','Claimant','Driver',
						        'Motor Vehicle Driver','Motorcycle Rider',
						        'Bicycle Pillion','Pedestrian','Cyclist')
    GROUP BY 
		a.claim_number,  
		a.policy_number,
		a.claim_lodgement_date,
	        b.contact_id
),

injured_party_summary AS (
	SELECT  claim_number,
		policy_number,
		claim_lodgement_date,
		SUM (injured_flag) AS num_injured_party,
		CASE WHEN COUNT(DISTINCT contact_id)- SUM(injured_flag)=1
					AND MAX(insured_injured_flag)=0
					AND MAX(insured_at_fault_flag)=1
					AND MAX(insured_driver_flag)=1
		THEN 1 ELSE 0
		END AS not_injured_is_only_insured_driver,
		CASE WHEN COUNT(DISTINCT contact_id)- SUM(injured_flag)=1
					AND MAX(insured_injured_flag)=0
					AND MAX(insured_at_fault_flag)=1
					AND MAX(insured_passenger_flag)=1
		THEN 1 ELSE 0
		END AS not_injured_is_only_insured_passenger	   
	FROM injured_detailed_info
	GROUP BY claim_number,
		 policy_number,
		 claim_lodgement_date
),

ais_code_summary AS (
	SELECT claim_number,
	MAX(COALESCE (ais_primary_injury_code IN ('F06','F31', 'F32','F33','F40','F411','F412','F413','F430','F431','F432','F91','F92','F93','R579','Z865')::int,0) ) AS primary_coded_psych_flag,
	MAX(COALESCE (ais_secondary_injury_code IN ('F06','F31', 'F32','F33','F40','F411','F412','F413','F430','F431','F432','F91','F92','F93','R579','Z865')::int, 0)) AS secondary_coded_psych_flag,
	(SUM(COALESCE (ais_primary_injury_code IN ('F06','F31', 'F32','F33','F40','F411','F412','F413','F430','F431','F432','F91','F92','F93','R579','Z865')::int,0))::FLOAT / NULLIF(count(DISTINCT exposure_id),0))
	:: FLOAT AS primary_psy_propotion,
	(SUM(COALESCE (ais_secondary_injury_code IN ('F06','F31', 'F32','F33','F40','F411','F412','F413','F430','F431','F432','F91','F92','F93','R579','Z865')::int,0))::FLOAT / NULLIF(count(DISTINCT exposure_id),0))
	:: FLOAT AS secondary_psy_propotion
	FROM pub.mv_ctp_claim_summary
	GROUP BY claim_number

)

SELECT  
	a.claim_number,
	a.policy_number,
	a.claim_lodgement_date,
	a.injured_person_amt,
	a.max_days_of_delay_to_hospital,
	a.max_days_of_minor_assess_to_lodgement,
	a.max_days_of_hospital_stay_following_the_accident,
	a.go_hospital_after_accident_amt,
	a.high_risk_amt,
	a.proportion_of_high_risk,
	a.minor_injury_amt,
	a.proportion_of_minor_injury,
	a.not_threshold_injury_amt,
	a.proportion_of_not_threshold_injury,
	a.treated_at_hospital_amt,
	a.proportion_of_coding_finalised,
	a.fatal_amt,
	a.prior_disability_amt,
	a.psycho_injury_amt,
	a.proportion_of_psych_injury,
	a.not_threshold_psych_injury_amt,
	a.proportion_of_not_threshold_psych_injury,
	a.non_demonstrable_injury_flag_amt,
	a.not_attend_hospital_immediately_amt,
	a.serious_injury_not_attend_hospital_imm_amt,			
	a.minor_injury_attend_hospital_amt,		
	a.serious_injury_no_ambulance_amt,	
	a.minor_injury_with_ambulance_transport_amt,
	a.ambulance_attendance_amt,
	a.ambulance_attendance_flag,
	a.ambulance_required_amt,
	a.ambulance_without_hospitalisation_amt,
	a.hospitalised_over_24_hours_amt,
	a.hospital_conflict_amt,
	a.disabled_due_to_accident_amt,
	a.no_helment_worn_amt,
	a.seatbelt_unfastened_amt,
	a.alcohol_consumed_amt,
	b.num_injured_party,
	b.not_injured_is_only_insured_driver,
	c.primary_coded_psych_flag,
	c.secondary_coded_psych_flag,
	c.primary_psy_propotion,
	c.secondary_psy_propotion

FROM cte_medical_summery a
LEFT JOIN injured_party_summary b
ON a.claim_number = b.claim_number
LEFT JOIN ais_code_summary c
ON a.claim_number = c.claim_number

