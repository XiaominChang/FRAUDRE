
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: shared pairs of doctor and repairer data for building CTP networks
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     11/07/2025   Xiaomin Chang             Initial release
-- -------------------------------------------------------------------------

{{ config(
    materialized='table',
    distributed_by=['claim_number'],
    post_hook=grant_access(this)
) }}

WITH ctp_vehicle_table AS (
	SELECT  a.claim_number, 
			a.claim_loss_date,
			CAST(b.at_fault_claim_exposure_id AS INT) AS claim_exposure_id,
	        b.at_fault_vehicle_id AS vehicle_id, 
	        c.vehicle_rego_number
	FROM ctx.mv_cc_ci_claim_header_ext AS a
	INNER JOIN pub_core.mv_ctp_vehicle_at_fault AS b
	ON a.claim_number = b.claim_number
	INNER JOIN pub.ctp_vehicle c
	ON b.claim_number = c.claim_number 
	AND b.at_fault_vehicle_id = c.vehicle_id 
	WHERE 	
	        claim_status_name <> 'Draft' 
		    AND ctp_statutory_insurer_state_name IN ('NSW')
		    AND a.line_of_business_name = 'Compulsory Third Party'
		    AND notify_only_claim_flag = 'No'
            AND (claim_closed_outcome_name IS NULL OR claim_closed_outcome_name='Completed')
		    AND vehicle_rego_number IS NOT NULL
            AND LOWER(vehicle_rego_number) NOT IN ('unknown', 'unknow', 'uniden', 'unreg', 'noreg1', 'nds*11','XXXXXXXXXX') 
		    AND b.at_fault_claim_exposure_id IS NOT NULL 
),

motor_vehicle_table AS (
        SELECT  a.claim_number, 
        		a.claim_loss_date,
                vehicle_id, 
                vehicle_rego_number
        FROM ctx.mv_cc_ci_claim_header_ext AS a
        INNER JOIN ctx.mv_cc_ci_incident_driver_vehicle_ext b
        ON a.claim_number = b.claim_number
        WHERE 
		    a.claim_loss_type_name = 'Motor' 
		AND a.policy_issue_state='NSW' 
		AND a.notify_only_claim_flag = 'No'
		AND a.claim_closed_outcome_name not in ('Duplicate' ,'Open in error')
),

repairer_claims AS (
	SELECT  a.claim_number,
			a.claim_exposure_id,
            c.repairer_code, 
            c.repairer_name_code,
            c.repairer_name_group,
            c.repairer_name
	FROM ctp_vehicle_table a
	INNER JOIN motor_vehicle_table b
	ON a.vehicle_rego_number = b.vehicle_rego_number
	AND a.claim_loss_date = b.claim_loss_date
	INNER JOIN pub.mv_motor_assessing_monthly_historical c
	ON b.claim_number = c.claim_number
    WHERE c.repairer_relationship_type_code !='AMC'
),

doctor_claims AS (
    -- Get all claims associated with doctors
    SELECT 
        claim_number,
        claim_exposure_id, 
        fixed_contact_number AS doctor_contact_number,
        fixed_email_address AS doctor_email_address,
        fixed_contact_name AS doctor_contact_name,
        full_address AS doctor_full_address

    FROM 
       {{ ref('ctp_contact_data_dbt') }} 
    WHERE 
        role_name = 'Doctor'
),



doctor_repairer_pairs AS (
    -- Find all pairs of claims that share both the same doctor and lawyer
    SELECT DISTINCT
        a.claim_number,
        a.claim_exposure_id,
        doctor_contact_number,
        doctor_email_address,
        doctor_contact_name,
        doctor_full_address,
        b.repairer_name_code,
        b.repairer_code,
        b.repairer_name      
    FROM 
        doctor_claims a
    JOIN 
        repairer_claims b 
    ON a.claim_number = b.claim_number
    AND a.claim_exposure_id = b.claim_exposure_id
)


SELECT  DISTINCT
    a.claim_number AS claim_number_1,
    a.claim_exposure_id AS claim_exposure_id_1,
    b.claim_number AS claim_number_2,
    b.claim_exposure_id AS claim_exposure_id_2,
     -- Doctor details
    a.doctor_contact_number AS doctor_contact_number,
    a.doctor_contact_name AS doctor_contact_name,
    -- repairer code
    a.repairer_name as repairer_name

FROM doctor_repairer_pairs a
INNER JOIN doctor_repairer_pairs b
ON (a.claim_number<>b.claim_number
OR a.claim_exposure_id<>b.claim_exposure_id)
AND (    a.doctor_contact_number = b.doctor_contact_number
        OR a.doctor_email_address = b.doctor_email_address 
        OR a.doctor_contact_name = b.doctor_contact_name 
        OR a.doctor_full_address = b.doctor_full_address)
AND a.repairer_name = b.repairer_name