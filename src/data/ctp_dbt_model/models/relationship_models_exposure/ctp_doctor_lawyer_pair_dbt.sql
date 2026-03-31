
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: shared pairs of doctor and lawyer data for building CTP networks
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     11/07/2025   Xiaomin Chang             Initial release
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

lawyer_claims AS (
    -- Get all claims associated with lawyers
    SELECT 
        claim_number, 
        claim_exposure_id,
        fixed_contact_number AS lawyer_contact_number,
        fixed_email_address AS lawyer_email_address,
        fixed_contact_name AS lawyer_contact_name,
        full_address AS lawyer_full_address
         
    FROM 
        {{ ref('ctp_contact_data_dbt') }} 
    WHERE 
        role_name IN ('Legal', 'Legal Representative', 'Lawyer')
        AND fixed_contact_name NOT IN ('Hall and Wilcox', 'Hall & Wilcox', 'Hall And Wilcox Lawyers','Hall And Wilcox')
),

doctor_lawyer_pairs AS (
    -- Find all pairs of claims that share both the same doctor and lawyer
    SELECT DISTINCT
        a.claim_number,
        a.claim_exposure_id,
        doctor_contact_number,
        doctor_email_address,
        doctor_contact_name,
        doctor_full_address,
        lawyer_contact_number,
        lawyer_email_address,
        lawyer_contact_name,
        lawyer_full_address
        
    FROM 
        doctor_claims a
    JOIN 
        lawyer_claims b 
    ON a.claim_number = b.claim_number
    AND a.claim_exposure_id = b.claim_exposure_id
)


SELECT DISTINCT
    a.claim_number AS claim_number_1,
    a.claim_exposure_id AS claim_exposure_id_1,
    b.claim_number AS claim_number_2,
    b.claim_exposure_id AS claim_exposure_id_2,
    -- Doctor details
    a.doctor_contact_number AS doctor_contact_number,
    a.doctor_contact_name AS doctor_contact_name,
    
    -- Lawyer details
    a.lawyer_contact_number AS lawyer_contact_number,
    a.lawyer_contact_name AS lawyer_contact_name
FROM 
    doctor_lawyer_pairs a
INNER JOIN doctor_lawyer_pairs b
ON (a.claim_number<>b.claim_number
OR a.claim_exposure_id<>b.claim_exposure_id)
AND ( a.doctor_contact_number = b.doctor_contact_number
	  OR a.doctor_email_address = b.doctor_email_address 
	  OR a.doctor_contact_name = b.doctor_contact_name 
      OR a.doctor_full_address = b.doctor_full_address)
AND ( a.lawyer_contact_number = b.lawyer_contact_number
	  OR a.lawyer_email_address = b.lawyer_email_address 
	  OR a.lawyer_contact_name = b.lawyer_contact_name 
      OR a.lawyer_full_address = b.lawyer_full_address)