
-- -- -------------------------------------------------------------------------------------------------
-- -- Author:      Xiaomin Chang
-- -- Description: Third-party features for CTP modelling                                                                               
-- -- -------------------------------------------------------------------------------------------------
-- -- -------------------------------------------------------------------------------------------------
-- -- VERSIONS   DATE        WHO         			  DESCRIPTION
-- -- 1.0        04/04/2024  Xiaomin Chang 	          Initial version
-- -- -------------------------------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['claim_number'],
		  post_hook = grant_access(this)
          ) }}



WITH claim_info AS (
    SELECT DISTINCT 
      claim_number,
      claim_lodgement_date,
      policy_number,
      claim_loss_date
    FROM
        ctx.mv_cc_ci_claim_header_ext 
    WHERE claim_status_name <> 'Draft' 
		  AND ctp_statutory_insurer_state_name IN ('NSW')
		  AND line_of_business_name = 'Compulsory Third Party'
		  AND notify_only_claim_flag = 'No'
	          AND claim_lodgement_date::date BETWEEN  '{{ var("claim_start_date") }}'  AND '{{ var("claim_end_date") }}' 
		  --  and  claim_lodgement_date::date BETWEEN  '{START_DATE}' AND '{END_DATE}'
		  AND (claim_closed_outcome_name IS NULL OR claim_closed_outcome_name='Completed')  
),

medical_provider_info as 
(
    SELECT 
      a.claim_number,
      a.claim_lodgement_date,
      b.provider_id,
	  b.exposure_id,
	  MIN (recovery_plan_start_date) AS recovery_plan_start_date,
	  MAX (recovery_plan_end_date) AS recovery_plan_end_date,
	  EXTRACT(DAY FROM (MAX(b.recovery_plan_create_date - a.claim_loss_date))) AS days_late_recovery_from_loss  
    FROM
        claim_info a
    LEFT JOIN  ctx.mv_cc_ci_ctp_trac_ext b
	ON  a.claim_number=b.claim_number
  WHERE b.provider_id IS NOT NULL 
  GROUP BY    a.claim_number,
		      a.claim_lodgement_date,
		      b.provider_id,
			  b.exposure_id
),

medical_provider_summary AS (
	  SELECT  claim_number,
		      claim_lodgement_date,
		      provider_id,
		      COUNT(claim_number) OVER (PARTITION BY provider_id ORDER BY claim_lodgement_date) AS num_hist_claims_with_same_provider,
		      COUNT(DISTINCT exposure_id) AS injured_with_same_provider_amt,
		      EXTRACT(DAY FROM MAX(recovery_plan_end_date-recovery_plan_start_date)) AS max_recovery_treatment_days,
		      EXTRACT(DAY FROM AVG(recovery_plan_end_date-recovery_plan_start_date)) AS	avg_recovery_treatment_days,
		      MAX(days_late_recovery_from_loss) AS max_days_late_recovery_from_loss,
		      AVG(days_late_recovery_from_loss) AS avg_days_late_recovery_from_loss
	  FROM medical_provider_info
	  GROUP BY claim_number,
			   claim_lodgement_date,
			   provider_id
),

medical_provider_summary_cust AS (
	  SELECT  claim_number,
		      claim_lodgement_date,
		      COUNT(DISTINCT provider_id) AS amt_distinct_medical_provider,
		      MAX (injured_with_same_provider_amt) AS max_injured_with_same_provider_amt,
		      SUM (CASE WHEN injured_with_same_provider_amt >1 
		      		THEN 1 ELSE 0 END) AS providers_with_multiple_injured_amt,
		      MAX(max_recovery_treatment_days) AS max_recovery_treatment_days,
		      AVG(avg_recovery_treatment_days)::int AS avg_recovery_treatment_days,
		      MAX(max_days_late_recovery_from_loss) AS max_days_late_recovery_from_loss,
		      AVG(avg_days_late_recovery_from_loss)::int AS avg_days_late_recovery_from_loss
	  FROM medical_provider_summary
	  GROUP BY claim_number,
		       claim_lodgement_date
),


third_party_trans_info as(

SELECT   a.claim_number,
		 a.claim_lodgement_date,
		 b.bank_bsb_number,
		 b.bank_account_number,
		 b.bank_bsb_number || b.bank_account_number AS account_num,
		 b.transaction_method,
		 b.transaction_date,
		 c.provider_id,
		 ROW_NUMBER() OVER (PARTITION BY a.claim_number, c.provider_id ORDER BY b.transaction_date) trans_row_num,
		 ROW_NUMBER() OVER (PARTITION BY a.claim_number, b.bank_bsb_number || b.bank_account_number ORDER BY b.transaction_date) account_row_num
FROM claim_info a
INNER JOIN pub.mv_fraud_investigations_payments_analysis b
ON a.claim_number = b.claim_number 
INNER JOIN ctx.mv_cc_ci_claim_contact_ext c
ON a.claim_number =c.claim_number 
AND b.payee_contact_id = c.claim_contact_id
),


distinct_account_third_party AS (
SELECT  claim_number,
		provider_id,
		COUNT(DISTINCT account_num) AS num_account_same_medical_provider
FROM third_party_trans_info 
WHERE transaction_method='eft'
GROUP BY claim_number,
		 provider_id
),

distinct_account_third_party_cust AS (
SELECT
	claim_number,
	MAX(CASE WHEN num_account_same_medical_provider>1 THEN 1 ELSE 0 END) AS diff_account_same_medical_provider_flag
FROM distinct_account_third_party 
GROUP BY claim_number
),

distinct_claim_third_party AS (
SELECT 
	provider_id,
	transaction_date,
	SUM(CASE WHEN trans_row_num=1 THEN 1 ELSE 0 END)
	OVER (PARTITION BY provider_id ORDER BY transaction_date) AS claims_same_provider_amt,
	SUM(CASE WHEN account_row_num=1 THEN 1 ELSE 0 END)
	OVER (PARTITION BY provider_id ORDER BY transaction_date) AS account_same_provider_amt
FROM third_party_trans_info
),

distinct_claim_third_party_cust AS (
SELECT  a.claim_number,
		MAX(claims_same_provider_amt) AS max_claims_same_provider_amt,
		MAX(account_same_provider_amt) AS max_accounts_same_provider_amt
FROM ( SELECT claim_number,
			  provider_id,
			  claim_lodgement_date
	   FROM third_party_trans_info
	   GROUP BY claim_number,
				provider_id,
				claim_lodgement_date) AS a
INNER JOIN distinct_claim_third_party b
ON a.provider_id=b.provider_id
   AND b.transaction_date < a.claim_lodgement_date 
GROUP BY claim_number
)






SELECT  a.claim_number,
		a.claim_lodgement_date,
		a.amt_distinct_medical_provider,
		a.max_injured_with_same_provider_amt,
		a.providers_with_multiple_injured_amt,
		a.max_recovery_treatment_days,
		a.avg_recovery_treatment_days,
		a.max_days_late_recovery_from_loss,
		a.avg_days_late_recovery_from_loss,
		COALESCE(c.diff_account_same_medical_provider_flag, 0) AS diff_account_same_medical_provider_flag,
		COALESCE(d.max_claims_same_provider_amt, 0) AS max_claims_same_provider_amt,
		COALESCE(d.max_accounts_same_provider_amt, 0) AS max_accounts_same_provider_amt
FROM medical_provider_summary_cust a
LEFT JOIN distinct_account_third_party_cust c
ON a.claim_number= c.claim_number
LEFT JOIN distinct_claim_third_party_cust d
ON a.claim_number= d.claim_number