-- -------------------------------------------------------------------------------------------------
-- Author:      Anahita Namvar
-- Description: Policies & derived features for CTP Anomaly Detection Model                                                                                
-- -------------------------------------------------------------------------------------------------
-- VERSIONS   DATE        WHO         	      DESCRIPTION
-- 1.0        23/02/2024  Anahita Namvar   Initial version.
-- 2.0        22/03/2024  Anahita Namvar   updated
-- -------------------------------------------------------------------------------------------------

{{ config(
          materialized='table',
		  distributed_by = ['policy_number'],
          post_hook = grant_access(this)
          ) }}

-- Policy data with claim number and as at date attached
-- Many rows per policy
-- NOTE: Left join onto all policy transactions and then filter policies to get correct
-- risk associated with the claim - much faster than a left join that uses case when.
-- Latest policy transaction row corresponding to each claim
-- One row per claim
 
 with policy_data_interest AS (
	SELECT
		clm_header.claim_number,
		clm_header.claim_loss_date AS as_at_date,
		policy_data.registration_number AS policy_vehicle_rego_number,
		policy_data.*
	FROM
		{{ ref('ctp_claims_data_dbt') }} AS clm_header
	LEFT JOIN {{ ref('ctp_policy_huon_dbt') }} AS policy_data ON
		policy_data.policy_number = clm_header.policy_number
		AND policy_data.policy_period_edit_effective_date <= clm_header.claim_loss_date
		AND policy_data.policy_period_edit_effective_date >= clm_header.claim_loss_date - interval '730 DAY'
-- Many rows per policy
)
,
-- Calculate the distinct number of registration numbers for the policies

 distinct_count as (
           SELECT 
        claim_number,
        COUNT(DISTINCT registration_number) AS count_distinct_registration_number,
        count (DISTINCT policy_number) as count_distinct_policy_number
    FROM policy_data_interest
    GROUP BY claim_number ),
        
policy_data_interest_no_dups as (
SELECT policy_data_interest.*
FROM 
    policy_data_interest
LEFT join 
distinct_count
    ON
    policy_data_interest.claim_number = distinct_count.claim_number 

--    EXTRACT('year' FROM policy_data_interest.term_start_date) = excess_table.term_year AND
--    EXTRACT('month' FROM policy_data_interest.term_start_date) = excess_table.term_month
), 

policy_data_interest_risk AS
	(
		SELECT
			DISTINCT ON
			(claim_number)
		*
		FROM
			policy_data_interest_no_dups AS policy_data
		WHERE
			policy_period_edit_effective_date <= as_at_date
		ORDER BY
			claim_number,
			job_number DESC
		--	version_transaction_date DESC
			--  "version_transaction_date" does not exist : error
	),	
-- Find changes in risk address, excess, total_sum_insured, and vehicle modifications made to policy
-- Many rows per policy
-- policy_demerit_points, client_conviction_points are null 
policy_changes AS (
	SELECT 
		claim_number, 
		policy_number, 
		registration_number,
		policy_period_edit_effective_date,
		-- Get previous value of features by checking if the value was different to the current value
		
		NULLIF(
			LAG(suburb) OVER 
            (
				PARTITION BY 
              claim_number
			ORDER BY
				claim_number,
				policy_period_edit_effective_date ASC
			),
			suburb
		) AS previous_suburb_if_changed,
		NULLIF(
			LAG(postal_code) OVER 
            (
				PARTITION BY 
              claim_number
			ORDER BY
				claim_number,
				policy_period_edit_effective_date ASC
			),
			postal_code
		) as previous_postal_code_changed
		
	FROM
		policy_data_interest_no_dups
),

-- Compute derived features for entire policy history
-- Many rows per policy
-- policy_derived AS (
-- 	SELECT
-- 		a.policy_number,
-- 		b.claim_number,
-- 		a.policy_period_edit_effective_date,
-- 		previous_postal_code_changed,
-- 		-- Find the time difference between claim lodgment and the policy change
-- 		CASE WHEN previous_postal_code_changed IS NOT NULL THEN 1 ELSE 0 END AS previous_post_code_changed_flag,
-- 		CASE WHEN previous_suburb_if_changed IS NOT NULL THEN 1 ELSE 0 END AS previous_suburb_changed_flag,
-- 		CASE
-- 		WHEN previous_postal_code_changed IS NOT NULL THEN as_at_date - a.policy_period_edit_effective_date
-- 		END AS time_since_postcode_change,
-- 		CASE
-- 			WHEN previous_suburb_if_changed IS NOT NULL THEN as_at_date - a.policy_period_edit_effective_date
-- 		END AS time_since_suburb_change
-- 		FROM
-- 		policy_data_interest_no_dups AS a
-- 		LEFT JOIN policy_changes AS b ON
-- 			a.policy_number = b.policy_number
-- 		AND a.policy_period_edit_effective_date = b.policy_period_edit_effective_date
-- ) ,


policy_derived AS (
	SELECT
		a.policy_number,
		b.claim_number,
		a.policy_period_edit_effective_date,
		previous_postal_code_changed,
		-- Find the time difference between claim lodgment and the policy change
    CASE
		WHEN previous_postal_code_changed IS NOT NULL THEN as_at_date - a.policy_period_edit_effective_date
	END AS time_since_postcode_change,
	previous_suburb_if_changed,
	CASE
		WHEN previous_suburb_if_changed IS NOT NULL THEN as_at_date - a.policy_period_edit_effective_date
	END AS time_since_suburb_change
  	FROM
    	policy_data_interest_no_dups AS a
  	LEFT JOIN policy_changes AS b ON
		a.policy_number = b.policy_number
        AND a.policy_period_edit_effective_date = b.policy_period_edit_effective_date
) ,

-- policy_derived_summary AS (
-- 	SELECT
-- 		policy_number,
-- 		claim_number,
-- 		max (previous_post_code_changed_flag) as previous_post_code_changed_flag,
-- 		max (previous_suburb_changed_flag) as previous_suburb_changed_flag,
-- 		max(time_since_postcode_change)
-- 	from policy_derived
-- 	group by policy_number,
-- 		 claim_number
-- ) ,

-- Find most recent policy changes
-- One row per policy
policy_derived_final AS (
	SELECT
		DISTINCT
    		policy_number,
		claim_number,
		-- Get first non-null value that corresponds to the latest policy change before lodgement
		FIRST_VALUE(previous_suburb_if_changed) OVER (
			PARTITION BY
				claim_number
			ORDER BY
				CASE
					WHEN previous_suburb_if_changed IS NOT NULL THEN policy_period_edit_effective_date
				END DESC NULLS LAST
			) AS previous_suburb_if_changed,
		FIRST_VALUE(time_since_suburb_change) OVER  (
			PARTITION BY
				claim_number
			ORDER BY
				CASE
					WHEN time_since_suburb_change IS NOT NULL THEN policy_period_edit_effective_date
				END DESC NULLS LAST
			) AS time_since_suburb_change,
		FIRST_VALUE(previous_postal_code_changed) OVER (
			PARTITION BY 
				claim_number
			ORDER BY
				CASE
					WHEN previous_postal_code_changed IS NOT NULL THEN policy_period_edit_effective_date
				END DESC NULLS LAST
			) AS previous_postal_code_changed,
		FIRST_VALUE(time_since_postcode_change) OVER (
			PARTITION BY 
				claim_number
			ORDER BY
				CASE
					WHEN time_since_postcode_change IS NOT NULL THEN policy_period_edit_effective_date
				END DESC NULLS LAST
			) AS time_since_postcode_change
		
	FROM
		policy_derived
) 
-- Final policy dataset with derived features
-- One row per policy corresponding to each claim in the claim header
SELECT
	a.*,
	previous_suburb_if_changed,
	time_since_suburb_change,
	previous_postal_code_changed,
	time_since_postcode_change
	
FROM
	policy_data_interest_risk AS a
LEFT JOIN policy_derived_final AS b
  ON
	a.claim_number = b.claim_number
