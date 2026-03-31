-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: shared participant data for building CTP networks
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     24/03/2025   Xiaomin Chang             Initial release
-- -------------------------------------------------------------------------

{{ config(
    materialized='table',
    distributed_by=['claim_number'],
    post_hook=grant_access(this)
) }}


With participant AS (
    SELECT DISTINCT
        claim_number, 
        fixed_contact_number AS participant_contact_number,
        fixed_email_address AS participant_email_address,
        fixed_contact_name AS participant_contact_name,
        full_address AS participant_full_address,
        role_name

    FROM 
       {{ ref('ctp_contact_data_dbt1') }} 
    WHERE 
        role_name IN ('Driver', 'Passenger',  'Claimant', 'Witness')
        -- role_name IN ('Driver', 'Passenger', 'Insured', 'Claimant', 'Witness')
)


SELECT DISTINCT
    a.claim_number AS claim_number_1,
    b.claim_number AS claim_number_2,
    -- COALESCE(a.role_name, b.role_name) as role_name,
    COALESCE(a.participant_contact_name, b.participant_contact_name) as cust_contact_name,
    COALESCE(a.participant_contact_number, b.participant_contact_number) as cust_contact_number

FROM 
    participant a
INNER JOIN participant b
ON a.claim_number<>b.claim_number
AND ( a.participant_contact_number = b.participant_contact_number
	  OR a.participant_email_address = b.participant_email_address 
	  OR a.participant_contact_name = b.participant_contact_name 
      OR a.participant_full_address = b.participant_full_address)



