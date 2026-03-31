-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                              
-- Description: Claims Payment Data for CTP Anomaly Detection Model 
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     04/03/2024   Xiaomin Chang             Initial release   
-- 2.00     20/03/2024   Xiaomin Chang             v2        
-- -------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['claim_number'],
		  post_hook = grant_access(this)
          ) }}

-- Aggregate claim payment data at policy level and apply to claims
WITH cte_claim_payments_aggregation AS (
	SELECT	a.claim_number,
			a.policy_number,
			a.claim_lodgement_date,
			COUNT(DISTINCT  bank_bsb_number || bank_account_number) AS num_distinct_bank_accounts_to_policy,
			SUM (payment_amount) AS  amt_historical_policy_payments,
			COUNT(1) AS num_claim_payments,
			COUNT(DISTINCT b.claim_number) AS num_paid_claims_to_policy,
			SUM(CASE WHEN b.policy_issue_state <> b.payee_state_name 
				THEN 1 ELSE 0 END) AS num_payee_policy_distinct_state,
			SUM(CASE WHEN SUBSTRING(b.payee_post_code FROM 1 FOR 1) <> SUBSTRING(b.claim_postcode FROM 1 FOR 1)
				THEN 1 ELSE 0 END) AS num_payee_claim_distinct_state,
			SUM(CASE WHEN b.payee_post_code <> b.claim_postcode
				THEN 1 ELSE 0 END) AS num_payees_claim_distinct_postcode,
			SUM(CASE WHEN b.claim_insured_contact_name <> b.claim_reported_by_name
				THEN 1 ELSE 0 END) AS num_payees_not_claim_reporter,
			SUM(CASE WHEN b.transaction_date between DATE(a.claim_lodgement_date - interval '1 year') and a.claim_lodgement_date 
				THEN 1 ELSE 0 END) AS num_payments_1_year,
			SUM(CASE WHEN b.transaction_date between DATE(a.claim_lodgement_date - interval '2 year') and a.claim_lodgement_date 
				THEN 1 ELSE 0 END) AS num_payments_2_year,
			SUM(CASE WHEN b.transaction_date between DATE(a.claim_lodgement_date - interval '5 year') and a.claim_lodgement_date 
				THEN 1 ELSE 0 END) AS num_payments_5_year,
			SUM(CASE WHEN b.transaction_date between DATE(a.claim_lodgement_date - interval '1 year') and a.claim_lodgement_date 
				THEN payment_amount ELSE 0 END) AS amt_claim_payments_1_year,
			SUM(CASE WHEN b.transaction_date between DATE(a.claim_lodgement_date - interval '2 year') and a.claim_lodgement_date 
				THEN payment_amount ELSE 0 END) AS amt_claim_payments_2_year,
			SUM(CASE WHEN b.transaction_date between DATE(a.claim_lodgement_date - interval '5 year') and a.claim_lodgement_date 
				THEN payment_amount ELSE 0 END) AS amt_claim_payments_5_year
	FROM
		ctx.mv_cc_ci_claim_header_ext AS a
	INNER JOIN 
		pub.mv_fraud_investigations_payments_analysis AS b
	ON
		a.policy_number = b.policy_number
		AND b.transaction_date < a.claim_lodgement_date
	WHERE
	        a.source_system in ('CC_CI')
        AND a.notify_only_claim_flag = 'No'
        AND a.line_of_business_name='Compulsory Third Party'
        AND a.policy_issue_state='NSW'
        AND a. policy_number not ilike '%NDS%'
	AND (a.claim_closed_outcome_name IS NULL OR a.claim_closed_outcome_name='Completed')
    GROUP BY a.claim_number,
			 a.policy_number,
			 a.claim_lodgement_date
),


-- Create a cumulative sum of the number of policies which pay to each bank account as a function of time

cte_policies_by_same_account AS (
	SELECT
		transaction_date,
		bank_bsb_number,
		bank_account_number,
		policy_number,
		ROW_NUMBER() OVER (
			PARTITION BY bank_bsb_number,
			bank_account_number,
			policy_number
		ORDER BY
			transaction_date
		) AS row_num
	FROM
		pub.mv_fraud_investigations_payments_analysis 
	WHERE
		 bank_bsb_number IS not NULL
		AND bank_account_number IS not null
	GROUP BY
		transaction_date,
		bank_bsb_number,
		bank_account_number,
		policy_number
	ORDER BY
		transaction_date
),


cte_distinct_policies_transaction AS  (
	SELECT
		transaction_date,
		bank_bsb_number,
		bank_account_number,
		SUM(CASE WHEN row_num = 1 THEN 1 ELSE 0 END) OVER (
			PARTITION BY bank_bsb_number,
			bank_account_number
		ORDER BY
			transaction_date
		) AS num_policies_by_account
	FROM cte_policies_by_same_account
),

cte_policies_transaction_summary AS (
	SELECT
		a.claim_number,
		a.policy_number,
		a.claim_lodgement_date,
		MAX(COALESCE (b.num_policies_by_account,0)) AS max_policies_paid_to_same_bank_account,
		MAX(COALESCE (a.payment_amount,0)) AS max_single_payment_amt
	FROM
		pub.mv_fraud_investigations_payments_analysis as a
	INNER JOIN cte_distinct_policies_transaction AS b 
	ON
		a.bank_bsb_number = b.bank_bsb_number
		AND a.bank_account_number = b.bank_account_number
		AND b.transaction_date < a.claim_lodgement_date
	WHERE 	
	-- a. policy_number not ilike '%NDS%'
		a.line_of_business_name= 'Compulsory Third Party'
		AND a.policy_issue_state ='NSW'
	GROUP BY 
		a.claim_number,
		a.policy_number,
		a.claim_lodgement_date
)
			 
------------------------------------------------------------------
----- Merge on all columns, one row per claim/policy/lodgement-----
SELECT
	a.claim_number,
	a.policy_number,
	a.claim_lodgement_date,
	a.num_distinct_bank_accounts_to_policy,
	a.amt_historical_policy_payments,
	a.num_claim_payments,
	a.num_paid_claims_to_policy,
	a.num_payee_policy_distinct_state,
	a.num_payee_claim_distinct_state,
	a.num_payees_claim_distinct_postcode,
	a.num_payees_not_claim_reporter,
	a.num_payments_1_year,
	a.num_payments_2_year,
	a.num_payments_5_year,
	a.amt_claim_payments_1_year,
	a.amt_claim_payments_2_year,
	a.amt_claim_payments_5_year,
	b.max_policies_paid_to_same_bank_account,
	b.max_single_payment_amt
from	cte_claim_payments_aggregation AS a		
LEFT JOIN cte_policies_transaction_summary AS b
	ON
		a.claim_number = b.claim_number
	AND	a.policy_number = b.policy_number
	AND a.claim_lodgement_date = b.claim_lodgement_date
--ORDER BY num_policy_distinct_bank_accounts desc