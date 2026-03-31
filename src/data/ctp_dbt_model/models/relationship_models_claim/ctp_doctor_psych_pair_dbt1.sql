
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: shared pairs of doctor and psych data for building CTP networks
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     10/09/2024   Xiaomin Chang             Initial release
-- -------------------------------------------------------------------------

{{ config(
    materialized='table',
    distributed_by=['claim_number'],
    post_hook=grant_access(this)
) }}

WITH doctor_claims AS (
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
        role_name = 'Doctor'
        -- (role_name = 'Doctor' OR role_name = 'Medical')
        -- AND fixed_contact_name !='Ambulance Service of New South Wales'
),

psych_claims AS (
    -- Get all claims associated with psychs
    SELECT 
        claim_number, 
        fixed_contact_number AS psych_contact_number,
        fixed_email_address AS psych_email_address,
        fixed_contact_name AS psych_contact_name,
        full_address AS psych_full_address
         
    FROM 
        {{ ref('ctp_contact_data_dbt1') }} 
    WHERE 
        role_name = 'Psychologist'
),

doctor_psych_pairs AS (
    -- Find all pairs of claims that share both the same doctor and psych
    SELECT DISTINCT
        a.claim_number,
        doctor_contact_number,
        doctor_email_address,
        doctor_contact_name,
        doctor_full_address,
        psych_contact_number,
        psych_email_address,
        psych_contact_name,
        psych_full_address
        
    FROM 
        doctor_claims a
    JOIN 
        psych_claims b 
    ON a.claim_number = b.claim_number
)


SELECT DISTINCT
    a.claim_number AS claim_number_1,
    b.claim_number AS claim_number_2,
    -- Doctor details
    a.doctor_contact_number AS doctor_contact_number,
    a.doctor_contact_name AS doctor_contact_name,
    
    -- psych details
    a.psych_contact_number AS psych_contact_number,
    a.psych_contact_name AS psych_contact_name
FROM 
    doctor_psych_pairs a
INNER JOIN doctor_psych_pairs b
ON a.claim_number<>b.claim_number
AND ( a.doctor_contact_number = b.doctor_contact_number
	  OR a.doctor_email_address = b.doctor_email_address 
	  OR a.doctor_contact_name = b.doctor_contact_name 
      OR a.doctor_full_address = b.doctor_full_address)
AND ( a.psych_contact_number = b.psych_contact_number
	  OR a.psych_email_address = b.psych_email_address 
	  OR a.psych_contact_name = b.psych_contact_name 
      OR a.psych_full_address = b.psych_full_address)