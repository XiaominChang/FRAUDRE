-- -------------------------------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: extract the final node modelling data                                                                                
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- VERSIONS   DATE        WHO         	    DESCRIPTION
-- 1.0        14/07/2025  Xiaomin Chang     Initial version.
-- -------------------------------------------------------------------------------------------------
{{ config(
          materialized='table',
          distributed_by = ['claim_exposure_id'],
          post_hook = grant_access(this)
          ) }}


WITH cte_claim_info AS
(
    SELECT DISTINCT 
    	a.claim_number,
    	b.claim_exposure_id,
 		MAX(b.claim_exposure_lodgement_date) AS claim_exposure_lodgement_date,
 		MAX(b.claim_exposure_status_name) AS claim_exposure_status_name,
 		MAX(c.contact_full_name) AS contact_full_name,
 		MAX(COALESCE(c.contact_mobile_number, c.mobile_phone_number, c.work_phone_number, c.home_phone_number)) AS fixed_contact_number,
 		MAX(address_line_1 || ' ' || address_suburb_name ||' ' || address_state_name  ||' ' ||  address_post_code) AS full_address
    FROM
        ctx.mv_cc_ci_claim_header_ext a
    JOIN ctx.mv_cc_ci_claim_exposure_header_ext b
    ON a.claim_number = b.claim_number
    JOIN ctx.mv_cc_ci_claim_contact_ext AS c
	ON a.claim_number = c.claim_number
	AND b.exposure_id = c.exposure_id
	AND c.role_name = 'Claimant'
    WHERE 
        claim_status_name <> 'Draft' 
	    and ctp_statutory_insurer_state_name IN ('NSW')
	    and a.line_of_business_name = 'Compulsory Third Party'
	    and notify_only_claim_flag = 'No'
        and a.claim_lodgement_date::date BETWEEN '{{ var("claim_start_date") }}' AND CURRENT_DATE
        and (claim_closed_outcome_name IS NULL OR claim_closed_outcome_name='Completed')
    GROUP BY a.claim_number,
    		 b.claim_exposure_id
),

cte_injury_info as
(
	select mccs.claim_number, 
		   CAST(claim_exposure_id AS INT) AS claim_exposure_id,
		   max(case when mccs.was_not_fit_for_work_flag ='Y' then 1 else 0 end) as not_fit_for_work_flag,
		   max(case when mccs.estimate_of_injury_severity_name like '%Minor%' and mccs.was_not_fit_for_work_flag ='Y' then 1 else 0 end) as off_work_minor_injury_flag
	FROM pub.mv_ctp_claim_summary mccs
	WHERE claim_exposure_id IS NOT NULL 
	AND statutory_state ='NSW'	
	group by
			mccs.claim_number,
			mccs.claim_exposure_id	
),

cte_medical_info as(
   SELECT 
        a.claim_number,
        CAST(claim_exposure_id AS INT) AS claim_exposure_id,
		CASE WHEN minor_injury_decision_code = '0' THEN 1 ELSE 0 END AS not_threshold_injury_flag,
        CASE WHEN minor_injury_assessment_type_name IS NOT NULL
            THEN CAST(date_of_minor_injury_assessment_decision AS date) - CAST(b.claim_lodgement_date AS date) 
            ELSE 0 END AS days_of_minor_assess_to_lodgement
    FROM pub_core.claim_injury a
    INNER JOIN pub_core.mv_claim_header b ON a.claim_number = b.claim_number
    WHERE b.policy_issue_state = 'NSW'
    	  AND line_of_business_name = 'Compulsory Third Party'
    ORDER BY claim_number, claim_exposure_id
),

injured_employment_info as (
SELECT 	claim_number,
		CAST(claim_exposure_id AS INT) AS claim_exposure_id,
	   CASE WHEN work_capacity in ('26 - Not working - Has Work capacity','06 - Not Working') 
	   			or pre_accident_employment_status ilike '%17 - Not Working - receiving weekly payments from a previous accident%' 
	   THEN 1 ELSE 0 end AS suspacious_not_working_flag,
	   CASE WHEN work_capacity ILIKE '%Self Employed%' THEN 1 ELSE 0 end AS self_employed_flag,
	   COALESCE (pre_accident_total_average_weekly_earning,0) AS max_pre_accident_weekly_earning
FROM pub.mv_ctp_claim_summary
),


cte_fitness_certificate AS (
	SELECT 
	    a.claim_number,
	    CAST(claim_exposure_id AS INT) AS claim_exposure_id,
	    COALESCE(
	        CAST(DATE_PART('day', MAX(b.fitness_certificate_start_date - a.claim_loss_date)) AS INT), 
	        0
	    ) AS max_late_treatment_days,
	    COALESCE(COUNT(DISTINCT b.fitness_certificate_id), 0) AS amt_fitness_certificate
	FROM 
	    pub_core.mv_claim_header a
	INNER JOIN pub_core.claim_exposure_weekly_benefit_summary b
	    ON a.claim_number = b.claim_number
	GROUP BY 
	    a.claim_number,
	    b.claim_exposure_id
),


cof_issue_info AS (
        SELECT 
            a.claim_number,
            CAST(claim_exposure_id AS INT) AS claim_exposure_id,
            CASE 
                WHEN b.weekend_flag = 'Y' OR public_holiday_nsw_flag = 'Y' THEN 1 
                ELSE 0 
            END AS cof_holiday_weekend_flag,
            CASE 
                WHEN a.fitness_certificate_date_received - a.fitness_certificate_issue_date > INTERVAL '2 months' THEN 1 
                ELSE 0 
            END AS cof_issue_to_received_over_2m_flag
        FROM pub_core.claim_exposure_weekly_benefit_summary a
        LEFT JOIN pub.dim_date b
            ON CAST(a.fitness_certificate_issue_date AS DATE) = b.date 
),


reject_payment_invoice as( 
		SELECT 
            a.claim_number, 
            CAST(a.claim_exposure_id AS INT) AS claim_exposure_id,
            invoice_number,
            CASE 
                WHEN SUM(CASE WHEN transaction_status_code = 'Rejected' THEN 1 ELSE 0 END) > 8 THEN 1 
                ELSE 0 
            END AS rejected_pay_over_8_flag
        FROM pub_core.mv_claim_transaction a
        JOIN pub.mv_ctp_claim_summary b
        ON a.claim_number = b.claim_number
        AND a.claim_exposure_id = b.claim_exposure_id
        WHERE a.claim_exposure_id IS NOT NULL 
        AND a.invoice_number IS NOT NULL
        AND   b.statutory_state ='NSW'
        GROUP BY a.claim_number, invoice_number, a.claim_exposure_id
    ),
    
reject_payment_info AS (    
    SELECT claim_number,
    	   claim_exposure_id,
    	   max(rejected_pay_over_8_flag) AS rejected_pay_over_8_flag
   	FROM reject_payment_invoice
   	GROUP BY claim_number, claim_exposure_id
    ),   

gp_payment_expo AS (
		SELECT 
            claim_number,
            claim_exposure_id,
            SUM(
                CASE 
                    WHEN transaction_subtype_name ILIKE '%General Practitioner Consultation%' THEN account_amount 
                    ELSE 0 
                END
            ) AS gp_consult_payment_total,
            SUM(
                CASE 
                    WHEN transaction_subtype_code = 'GP05' THEN account_amount 
                    ELSE 0 
                END
            ) AS non_pre_approved_gp_case_total
        FROM pub.mv_ctp_payment_sla
        WHERE claim_exposure_id IS NOT NULL 
        GROUP BY claim_number, claim_exposure_id
    ),
    
gp_payment_info AS (    
    SELECT 
        a.claim_number,
        CAST(a.claim_exposure_id AS INT) AS claim_exposure_id,
        CASE 
            WHEN non_pre_approved_gp_case_total > gp_consult_payment_total THEN 1 
            ELSE 0 
        END
        AS gp05_pay_exceed_gp_consult 
    FROM pub.mv_ctp_claim_summary a
    LEFT JOIN gp_payment_expo b
        ON a.claim_number = b.claim_number
        AND a.claim_exposure_id = b.claim_exposure_id
    WHERE a.statutory_state = 'NSW'
),

investigation_info AS (
SELECT claim_number,
	   CAST(claim_exposure_id AS INT) AS claim_exposure_id,
	   CASE WHEN fraud_investigation_status IN ('Investigating','CTP Alleged','CTP Accepted')
	   THEN 1 ELSE 0 
	   END AS investigation_flag,
	   CASE WHEN fraud_investigation_status = 'CTP Alleged'
	   THEN 1 ELSE 0 
	   END AS fraud_flag
FROM pub.mv_fraud_investigations_cc_ci_ctp
)



SELECT DISTINCT 
    a.*,
    
    -- Injury Info
    b.not_fit_for_work_flag,
    b.off_work_minor_injury_flag,
    
    -- Medical Info
    c.not_threshold_injury_flag,
    c.days_of_minor_assess_to_lodgement,
    
    -- Employment Info
    d.suspacious_not_working_flag,
    d.self_employed_flag,
    d.max_pre_accident_weekly_earning AS pre_accident_weekly_earning,
    
    -- Fitness Certificate Info
    e.max_late_treatment_days AS late_treatment_days,
    e.amt_fitness_certificate,
    
    -- Cause of Fraud Indicators
    f.cof_holiday_weekend_flag,
    f.cof_issue_to_received_over_2m_flag,
    
    -- Payment Rejection Info
    g.rejected_pay_over_8_flag,
    
    -- GP Payment Info
    h.gp05_pay_exceed_gp_consult,

	COALESCE(i.investigation_flag, 0) AS investigation_flag,
	COALESCE(i.fraud_flag, 0) AS fraud_flag

FROM 
    cte_claim_info a

-- Join with injury information
LEFT JOIN cte_injury_info b
    ON a.claim_number = b.claim_number
   AND a.claim_exposure_id = b.claim_exposure_id

-- Join with medical information
LEFT JOIN cte_medical_info c
    ON a.claim_number = c.claim_number
   AND a.claim_exposure_id = c.claim_exposure_id

-- Join with injured party employment details
LEFT JOIN injured_employment_info d
    ON a.claim_number = d.claim_number
   AND a.claim_exposure_id = d.claim_exposure_id

-- Join with fitness certificate info
LEFT JOIN cte_fitness_certificate e
    ON a.claim_number = e.claim_number
   AND a.claim_exposure_id = e.claim_exposure_id

-- Join with cause-of-fraud indicators
LEFT JOIN cof_issue_info f
    ON a.claim_number = f.claim_number
   AND a.claim_exposure_id = f.claim_exposure_id

-- Join with rejected payment info
LEFT JOIN reject_payment_info g
    ON a.claim_number = g.claim_number
   AND a.claim_exposure_id = g.claim_exposure_id

-- Join with GP payment info
LEFT JOIN gp_payment_info h
    ON a.claim_number = h.claim_number
   AND a.claim_exposure_id = h.claim_exposure_id

LEFT JOIN investigation_info i 
    ON a.claim_number = i.claim_number 
   AND a.claim_exposure_id = i.claim_exposure_id







