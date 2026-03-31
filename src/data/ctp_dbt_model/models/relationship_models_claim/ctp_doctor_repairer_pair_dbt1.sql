
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: shared pairs of doctor and repairer data for building CTP networks
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     10/09/2024   Xiaomin Chang             Initial release
-- -------------------------------------------------------------------------

{{ config(
    materialized='table',
    distributed_by=['claim_number'],
    post_hook=grant_access(this)
) }}

WITH ctp_vehicle_table AS (
        SELECT  a.claim_number, 
        		a.claim_loss_date,
                b.vehicle_id, 
                b.vehicle_vin,
                b.vehicle_rego_number
        FROM pub.mv_ctp_claim  AS a
        INNER JOIN ctx.mv_cc_ci_incident_driver_vehicle_ext b
        ON a.claim_number = b.claim_number
),

motor_vehicle_table AS (
        SELECT  a.claim_number, 
        		a.claim_loss_date,
                vehicle_id, 
                vehicle_vin,
                vehicle_rego_number
        FROM pub_core.mv_claim_header AS a
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
            c.repairer_code, 
            c.repairer_name_code,
            c.repairer_name_group,
            c.repairer_name
	FROM ctp_vehicle_table a
	INNER JOIN motor_vehicle_table b
	ON a.vehicle_vin = b.vehicle_vin
	AND a.claim_loss_date = b.claim_loss_date
	INNER JOIN pub.mv_motor_assessing_monthly_historical c
	ON b.claim_number = c.claim_number
),

doctor_claims AS (
    -- Get all claims associated with doctors
    SELECT 
        claim_number, 
        fixed_contact_number AS doctor_contact_number,
        fixed_email_address AS doctor_email_address,
        fixed_contact_name AS doctor_contact_name,
        full_address AS doctor_full_address

    FROM 
       {{ ref('ctp_contact_data_dbt1') }} 
    WHERE 
        -- (role_name = 'Doctor' OR role_name = 'Medical')
        -- AND fixed_contact_name !='Ambulance Service of New South Wales'
        role_name = 'Doctor'
),



doctor_repairer_pairs AS (
    -- Find all pairs of claims that share both the same doctor and lawyer
    SELECT DISTINCT
        a.claim_number,
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
)


SELECT  DISTINCT
    a.claim_number AS claim_number_1,
    b.claim_number AS claim_number_2,
     -- Doctor details
    a.doctor_contact_number AS doctor_contact_number,
    a.doctor_contact_name AS doctor_contact_name,
    -- repairer code
    a.repairer_name as repairer_name

FROM doctor_repairer_pairs a
INNER JOIN doctor_repairer_pairs b
ON a.claim_number <> b.claim_number
AND (    a.doctor_contact_number = b.doctor_contact_number
        OR a.doctor_email_address = b.doctor_email_address 
        OR a.doctor_contact_name = b.doctor_contact_name 
        OR a.doctor_full_address = b.doctor_full_address)
AND a.repairer_code = b.repairer_code

