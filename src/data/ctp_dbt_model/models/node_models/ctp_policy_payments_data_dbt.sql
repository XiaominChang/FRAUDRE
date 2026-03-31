-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                              
-- Description: Policy Payments Data for Motor Anomaly Detection Model  
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     01/03/2024   Xiaomin Chang             Initial release        
-- -------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['policy_number'],
          post_hook = grant_access(this)
          ) }}

with cte_premium_merge_claim AS
(
	SELECT
		  che.claim_number
		, che.policy_number
		, che.claim_lodgement_date
		, che.claim_loss_date
		, fcp.payment_type_cd
		, fcp.payment_type_cv
		, fcp.bank_code
		, fcp.account_no
		, fcp.card_obfuscated_no
		, fcp.payment_dt
		, fcp.last_payment_dt
		, fcp.next_payment_dt
		, fcp.payment_amt
		, fcp.payment_frequency
		, fcp.policy_party_rk
	FROM
		   ctx.mv_cc_ci_claim_header_ext AS che
		   	-- dl_analytics.fawe_fraud_ctp_policy_payments 
	INNER JOIN {{ ref('ctp_policy_transaction_payment_dbt') }} AS fcp   
	ON
		che.policy_number = substring(
			fcp.policy_identifier,
			6,
			character_length(fcp.policy_identifier)
		)
			AND CAST(
				fcp.payment_dt AS date
			) < che.claim_lodgement_date
	WHERE che.source_system in ('CC_CI')
        AND che.notify_only_claim_flag = 'No'
        AND che.line_of_business_name='Compulsory Third Party'
        AND che.policy_issue_state='NSW'
	AND (che.claim_closed_outcome_name IS NULL OR che.claim_closed_outcome_name='Completed')
),


cte_premium_payments_by_policy AS
(
	SELECT
		  claim_number
		, policy_number
		, claim_lodgement_date
		, claim_loss_date
		, payment_type_cd
		, payment_type_cv
		, bank_code
		, account_no
		, card_obfuscated_no
		, payment_dt
		, last_payment_dt
		, payment_frequency
		, policy_party_rk
		, row_number() over (partition by claim_number, policy_number order by payment_dt desc) as row_no_paydt
		, row_number() over (partition by claim_number, policy_number, policy_party_rk order by payment_dt desc) as row_no_recent_paydt
		, COUNT(DISTINCT policy_party_rk) over (partition by claim_number, policy_number) as previous_policy_holder_amt
		, COUNT(DISTINCT payment_dt) over (partition by claim_number, policy_number, policy_party_rk) as client_payment_times
		, CASE WHEN payment_frequency = '6 month' 
		  THEN (payment_dt - last_payment_dt)*2/365.0
		  when payment_frequency = '1 year'
		  THEN (payment_dt - last_payment_dt)/365.0
		  ELSE NULL 
		  END AS proportion_since_previous_payment
		, greatest( 
			  CASE WHEN payment_frequency = '6 month' 
			  THEN (payment_dt - last_payment_dt)-(365.0/2)
			  when payment_frequency = '1 year'
			  THEN (payment_dt - last_payment_dt)-365
			  ELSE 0 end , 0) as days_late_on_previous_payment	
	    , CASE WHEN payment_dt between DATE(claim_lodgement_date - interval '1 year') and claim_lodgement_date THEN 1
		  ELSE 0
		  END AS payment_flag_1_year
		, CASE WHEN payment_dt between DATE(claim_lodgement_date - interval '2 year') and claim_lodgement_date THEN 1
		  ELSE 0
		  END AS payment_flag_2_year
		, CASE WHEN payment_dt between DATE(claim_lodgement_date - interval '5 year') and claim_lodgement_date THEN 1
		  ELSE 0
		  END AS payment_flag_5_year		  
	FROM
		   cte_premium_merge_claim
), 

cte_non_null_premium_payments_by_policy AS (
	SELECT
		claim_number,
		policy_number,
		claim_lodgement_date,
		payment_type_cd,
		payment_type_cv,
		bank_code,
		account_no,
		card_obfuscated_no,
		payment_dt,
		payment_frequency,
		policy_party_rk,
		row_number() OVER (PARTITION BY claim_number, policy_number ORDER by payment_dt DESC) AS row_no_paydt
	FROM
		cte_premium_merge_claim
	WHERE
		(
			payment_type_cd IS NOT NULL
				OR payment_type_cv IS NOT NULL
				OR bank_code IS NOT NULL
				OR account_no IS NOT NULL
				OR card_obfuscated_no IS NOT NULL
		)
),


cte_most_recent_payment_method AS(
	SELECT
		  claim_number
		, policy_number
		, claim_lodgement_date
		, claim_loss_date
		, policy_party_rk
		, payment_type_cd
		, payment_type_cv
		, bank_code
		, account_no
		, card_obfuscated_no
		, payment_dt
--		, payment_amt
		, payment_frequency
		, cast(claim_lodgement_date as date)-payment_dt as days_lodgement_to_recent_payment
		, cast(claim_loss_date as date)-payment_dt as days_loss_to_recent_payment
	FROM
		cte_premium_payments_by_policy
	WHERE
		 row_no_paydt = 1
),

cte_most_recent_non_null_payment_method AS 
(
	SELECT
		 claim_number
		,policy_number
		,claim_lodgement_date
		,policy_party_rk
		,payment_type_cd
		,payment_type_cv
		,bank_code
		,account_no
		,card_obfuscated_no
		,payment_frequency
	FROM
		cte_non_null_premium_payments_by_policy
	WHERE
		row_no_paydt = 1
),


-- Determine timings between payments for each claim/policy/lodgement
cte_payment_timimgs AS
(
	SELECT
		 a.claim_number
		,a.policy_number
		,a.claim_lodgement_date
--		max(a.claim_loss_time_at_lodgement) AS claim_loss_time_at_lodgement
		,max(a.row_no_paydt) AS num_payments_made_on_policy
--		,max(a.row_no_recent_paydt) AS num_payments_by_recent_client
		,max(a.proportion_since_previous_payment) AS maximum_payment_term_proportion_between_payments
		,max(a.days_late_on_previous_payment) AS maximum_days_late_on_payments
		,avg(a.proportion_since_previous_payment) AS avg_payment_term_proportion_between_payments
		,avg(a.days_late_on_previous_payment) AS avg_days_late_on_payments
		,COUNT(DISTINCT a.policy_party_rk) AS sum_policy_payment_party
		,sum(a.payment_flag_1_year) AS amt_payments_1_year
		,sum(a.payment_flag_2_year) AS amt_payments_2_year
		,sum(a.payment_flag_5_year) AS amt_payments_5_year
		,max((CASE WHEN a.payment_flag_1_year = 1 THEN a.proportion_since_previous_payment ELSE 0 end )) AS max_payment_term_proportion_1_year
		,max((CASE WHEN a.payment_flag_2_year = 1 THEN a.proportion_since_previous_payment ELSE 0 end )) AS max_payment_term_proportion_2_year
		,max((CASE WHEN a.payment_flag_5_year = 1 THEN a.proportion_since_previous_payment ELSE 0 end )) AS max_payment_term_proportion_5_year
		,max((CASE WHEN a.payment_flag_1_year = 1 THEN a.days_late_on_previous_payment ELSE 0 end )) AS max_days_late_payment_1_year
		,max((CASE WHEN a.payment_flag_2_year = 1 THEN a.days_late_on_previous_payment ELSE 0 end )) AS max_days_late_payment_2_year
		,max((CASE WHEN a.payment_flag_5_year = 1 THEN a.days_late_on_previous_payment ELSE 0 end )) AS max_days_late_payment_5_year
	FROM
		cte_premium_payments_by_policy AS a
	INNER JOIN cte_most_recent_payment_method AS b
	ON
		a.claim_number = b.claim_number
		AND a.policy_number = b.policy_number
		AND a.claim_lodgement_date = b.claim_lodgement_date
	GROUP BY
		a.claim_number,
		a.policy_number,
		a.claim_lodgement_date 
),



cte_recent_payment_timimgs AS
(
	SELECT
		 a.claim_number
		,a.policy_number
		,a.claim_lodgement_date
--		max(a.claim_loss_time_at_lodgement) AS claim_loss_time_at_lodgement
		,max(a.row_no_recent_paydt) AS num_payments_by_recent_party
		,max(a.proportion_since_previous_payment) AS maximum_payment_term_proportion_between_payments_by_recent_party
		,max(a.days_late_on_previous_payment) AS maximum_days_late_on_payments_by_recent_party
		,avg(a.proportion_since_previous_payment) AS avg_payment_term_proportion_between_payments_by_recent_party
		,avg(a.days_late_on_previous_payment) AS avg_days_late_on_payments_by_recent_party
		,sum(a.payment_flag_1_year) AS amt_payments_1_year_by_recent_party
		,sum(a.payment_flag_2_year) AS amt_payments_2_year_by_recent_party
		,sum(a.payment_flag_5_year) AS amt_payments_5_year_by_recent_party
		,max((CASE WHEN a.payment_flag_1_year = 1 THEN a.proportion_since_previous_payment ELSE 0 end )) AS max_payment_term_proportion_1_year_by_recent_party
		,max((CASE WHEN a.payment_flag_2_year = 1 THEN a.proportion_since_previous_payment ELSE 0 end )) AS max_payment_term_proportion_2_year_by_recent_party
		,max((CASE WHEN a.payment_flag_5_year = 1 THEN a.proportion_since_previous_payment ELSE 0 end )) AS max_payment_term_proportion_5_year_by_recent_party
		,max((CASE WHEN a.payment_flag_1_year = 1 THEN a.days_late_on_previous_payment ELSE 0 end )) AS max_days_late_payment_1_year_by_recent_party
		,max((CASE WHEN a.payment_flag_2_year = 1 THEN a.days_late_on_previous_payment ELSE 0 end )) AS max_days_late_payment_2_year_by_recent_party
		,max((CASE WHEN a.payment_flag_5_year = 1 THEN a.days_late_on_previous_payment ELSE 0 end )) AS max_days_late_payment_5_year_by_recent_party
	FROM
		cte_premium_payments_by_policy a	
	INNER JOIN cte_most_recent_payment_method AS b
	ON
		a.claim_number = b.claim_number
		AND a.policy_number = b.policy_number
		AND a.claim_lodgement_date = b.claim_lodgement_date
		AND a.policy_party_rk = b.policy_party_rk
	GROUP BY
		a.claim_number,
		a.policy_number,
		a.claim_lodgement_date 
),

-- Determine DISTINCT payment methods for each claim/policy/lodgement
cte_distinct_payment_methods AS 
(
	SELECT
		DISTINCT
		claim_number,
		policy_number,
		claim_lodgement_date,
		payment_type_cd,
		payment_type_cv,
		bank_code,
		account_no,
		card_obfuscated_no
	FROM
		cte_premium_merge_claim
	WHERE
		(
			payment_type_cd IS NOT NULL
				OR payment_type_cv IS NOT NULL
				OR bank_code IS NOT NULL
				OR account_no IS NOT NULL
				OR card_obfuscated_no IS NOT NULL
		)
		AND payment_amt > 0
),

-- Determine number of DISTINCT payment methods used for each claim/policy/lodgement
cte_num_payment_methods_used AS 
(
	SELECT
		claim_number,
		policy_number,
		claim_lodgement_date,
		COUNT(*) AS policy_DISTINCT_payment_methods
	FROM
		cte_distinct_payment_methods
	GROUP BY
		claim_number,
		policy_number,
		claim_lodgement_date
),


cte_policy_same_payment_method AS
(
	SELECT
		 a.claim_number
		,a.policy_number
		,a.claim_lodgement_date
		,a.payment_type_cd
		,a.payment_type_cv
		,a.bank_code
		,a.account_no
		,a.card_obfuscated_no
		,COUNT(DISTINCT b.policy_number) as policy_amt_by_same_payment_method

	FROM
		cte_non_null_premium_payments_by_policy a
	inner join cte_non_null_premium_payments_by_policy b
	on
		(a.account_no=b.account_no or a.card_obfuscated_no=b.card_obfuscated_no)
	    and (a.claim_lodgement_date > b.payment_dt)
    group by 
    	 a.claim_number,
    	 a.policy_number,
    	 a.claim_lodgement_date,
    	 a.payment_type_cd,
    	 a.payment_type_cv,
    	 a.bank_code,
    	 a.account_no,
    	 a.card_obfuscated_no
    	 
),

cte_num_policy_same_payment_method AS
(
	SELECT   
			claim_number,
			policy_number,
			claim_lodgement_date,
			max(policy_amt_by_same_payment_method) AS max_policies_by_same_payment_method
	from cte_policy_same_payment_method
	group by 
			claim_number,
			policy_number,
			claim_lodgement_date
)


------------------------------------------------------------------
----- Merge on all columns, one row per claim/policy/lodgement-----
SELECT
	a.claim_number,
	a.policy_number,
	a.claim_lodgement_date,
	b.payment_frequency AS payment_frequency_at_claim,
	b.payment_dt AS final_payment_date_before_claim,
	b.days_lodgement_to_recent_payment,
	b.days_loss_to_recent_payment,
	c.payment_type_cd,
	c.payment_type_cv,
	c.bank_code,
	c.account_no,
	c.card_obfuscated_no,	
	d.num_payments_made_on_policy,
	d.maximum_payment_term_proportion_between_payments,
	d.maximum_days_late_on_payments,
	d.avg_payment_term_proportion_between_payments,
	d.avg_days_late_on_payments,
	d.sum_policy_payment_party,
	d.amt_payments_1_year,
	d.amt_payments_2_year,
	d.amt_payments_5_year,
	d.max_payment_term_proportion_1_year,
	d.max_payment_term_proportion_2_year,
	d.max_payment_term_proportion_5_year,
	d.max_days_late_payment_1_year,
	d.max_days_late_payment_2_year,
	d.max_days_late_payment_5_year,	
	e.num_payments_by_recent_party,
	e.maximum_payment_term_proportion_between_payments_by_recent_party,
	e.maximum_days_late_on_payments_by_recent_party,
	e.avg_payment_term_proportion_between_payments_by_recent_party,
	e.avg_days_late_on_payments_by_recent_party,
	e.amt_payments_1_year_by_recent_party,
	e.amt_payments_2_year_by_recent_party,
	e.amt_payments_5_year_by_recent_party,
	e.max_payment_term_proportion_1_year_by_recent_party,
	e.max_payment_term_proportion_2_year_by_recent_party,
	e.max_payment_term_proportion_5_year_by_recent_party,
	e.max_days_late_payment_1_year_by_recent_party,
	e.max_days_late_payment_2_year_by_recent_party,
	e.max_days_late_payment_5_year_by_recent_party,	
    	f.policy_DISTINCT_payment_methods,
    	g.max_policies_by_same_payment_method
FROM
--	dl_analytics.fa_fraud_adc_claims_features AS a
	ctx.mv_cc_ci_claim_header_ext as a
LEFT JOIN cte_most_recent_payment_method AS b
ON
	a.claim_number = b.claim_number
	AND a.policy_number = b.policy_number
	AND a.claim_lodgement_date = b.claim_lodgement_date
LEFT JOIN cte_most_recent_non_null_payment_method AS c
ON
	a.claim_number = c.claim_number
	AND a.policy_number = c.policy_number
	AND a.claim_lodgement_date = c.claim_lodgement_date
LEFT JOIN cte_payment_timimgs AS d
ON
	a.claim_number = d.claim_number
	AND a.policy_number = d.policy_number
	AND a.claim_lodgement_date = d.claim_lodgement_date
LEFT JOIN  cte_recent_payment_timimgs AS e
ON
	a.claim_number = e.claim_number
	AND a.policy_number = e.policy_number
	AND a.claim_lodgement_date = e.claim_lodgement_date
LEFT JOIN cte_num_payment_methods_used AS f
ON
	a.claim_number = f.claim_number
	AND a.policy_number = f.policy_number
	AND a.claim_lodgement_date = f.claim_lodgement_date
LEFT JOIN cte_num_policy_same_payment_method AS g
ON
	a.claim_number = g.claim_number
	AND a.policy_number = g.policy_number
	AND a.claim_lodgement_date = g.claim_lodgement_date	
WHERE
		a.source_system in ('CC_CI')
        AND a.notify_only_claim_flag = 'No'
        AND a.line_of_business_name='Compulsory Third Party'
        AND a.policy_issue_state='NSW'
	AND (a.claim_closed_outcome_name IS NULL OR a.claim_closed_outcome_name='Completed')

