
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                               
-- Description: This table is used to extract key features suggested by CTP
-- team
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     09/04/2025   Xiaomin Chang             Initial release        
-- -------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['claim_number'],
          post_hook = grant_access(this)
        ) }}

With cof_info_cte AS(
        SELECT 
                a.claim_number,
                CAST(a.fitness_certificate_issue_date AS DATE) AS fitness_certificate_issue_date,
                CAST(a.fitness_certificate_date_received AS DATE)fitness_certificate_date_received,
                CAST(a.fitness_certificate_start_date AS DATE) AS fitness_certificate_start_date, 
                CAST(a.fitness_certificate_end_date AS DATE) AS fitness_certificate_end_date,
                CASE WHEN b.weekend_flag='Y' OR public_holiday_nsw_flag ='Y'
                        THEN 1 ELSE 0 END AS cof_holiday_weekend_flag,
                CASE WHEN a.fitness_certificate_date_received - a.fitness_certificate_issue_date > INTERVAL '2 months'
                        THEN 1 ELSE 0 
                END AS cof_issue_to_received_over_2m_flag,
                b.date_day_short_desc,
                b.working_day_nsw_flag,
                b.weekend_flag,
                b.public_holiday_nsw_flag 
        FROM pub_core.claim_exposure_weekly_benefit_summary a
        LEFT JOIN pub.dim_date b
        ON CAST(a.fitness_certificate_issue_date AS DATE) = b.date 
),

cof_info_summary AS (
    SELECT a.claim_number,
           CASE WHEN max(cof_holiday_weekend_flag)=1 
           		THEN 1 ELSE 0 END AS cof_holiday_weekend_flag,
           CASE WHEN max(cof_issue_to_received_over_2m_flag)=1 
           		THEN 1 ELSE 0 END AS cof_issue_to_received_over_2m_flag
    FROM pub.mv_ctp_claim_summary  a
    LEFT JOIN cof_info_cte b
    ON a.claim_number = b.claim_number
    WHERE a.statutory_state ='NSW'
    GROUP BY a.claim_number 
),

reject_payment AS (
SELECT 
	   claim_number, 
	   invoice_number,
	   CASE WHEN (SUM(
	   		CASE WHEN transaction_status_code = 'Rejected' 
	   		THEN 1 ELSE 0 end)>8)
	   	THEN 1 ELSE 0 END AS rejected_pay_over_8_flag
FROM pub_core.mv_claim_transaction 
WHERE claim_number ILIKE '%NWR%' 
GROUP BY claim_number,
         invoice_number
),

reject_payment_summary AS (
SELECT a.claim_number, 
	   CASE WHEN 
	   	MAX (b.rejected_pay_over_8_flag)=1 
	   THEN 1 ELSE 0 END AS rejected_pay_over_8_flag
FROM pub.mv_ctp_claim_summary  a
LEFT JOIN reject_payment b
ON a.claim_number = b.claim_number
WHERE a.statutory_state ='NSW'
GROUP BY a.claim_number 
),

gp_payment_info AS(
	SELECT claim_number,
		   claim_exposure_id,
		   SUM(
		   	CASE WHEN transaction_subtype_name ILIKE '%General Practitioner Consultation%'
		   	THEN account_amount ELSE 0 END) AS gp_consult_payment_total,
	   	   SUM(
	   		CASE WHEN transaction_subtype_code='GP05'
	   		THEN account_amount ELSE 0 END) AS non_pre_approved_gp_case_total	   		
	FROM pub.mv_ctp_payment_sla
	GROUP BY claim_number,
			 claim_exposure_id
),

gp_payment_summary AS (
SELECT a.claim_number,
	   MAX (CASE WHEN non_pre_approved_gp_case_total > gp_consult_payment_total
	   THEN 1 ELSE 0 END) 
	   AS gp05_pay_exceed_gp_consult_flag 
FROM pub.mv_ctp_claim_summary  a
LEFT JOIN gp_payment_info b
ON a.claim_number = b.claim_number
AND a.claim_exposure_id = b.claim_exposure_id
WHERE a.statutory_state ='NSW'
GROUP BY a.claim_number 
)


SELECT a.claim_number,
	   COALESCE (b.cof_holiday_weekend_flag, 0) AS cof_holiday_weekend_flag,
	   COALESCE (b.cof_issue_to_received_over_2m_flag, 0) AS cof_issue_to_received_over_2m_flag,
	   COALESCE (c.rejected_pay_over_8_flag, 0) AS rejected_pay_over_8_flag,
	   COALESCE (d.gp05_pay_exceed_gp_consult_flag , 0) AS gp05_pay_exceed_gp_consult_flag
FROM ctx.mv_cc_ci_claim_header_ext as a
LEFT JOIN cof_info_summary b
ON a.claim_number = b.claim_number
LEFT JOIN reject_payment_summary c
ON a.claim_number = c.claim_number
LEFT JOIN gp_payment_summary d
ON a.claim_number = d.claim_number
WHERE
	a.source_system in ('CC_CI')
        AND a.notify_only_claim_flag = 'No'
        AND a.line_of_business_name='Compulsory Third Party'
        AND a.policy_issue_state='NSW'
	AND (a.claim_closed_outcome_name IS NULL OR a.claim_closed_outcome_name='Completed')
