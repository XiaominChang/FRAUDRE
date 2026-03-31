-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: full contact data for CTP network modelling
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     14/08/2024   Xiaomin Chang             Initial release
-- 2.00     10/09/2024   Xiaomin Chang             add new features
-- -------------------------------------------------------------------------

{{
    config(
        materialized = 'table',
        distributed_by = 'claim_number',
        post_hook = grant_access(this)
    )
}}
WITH cte_contact_full AS (
SELECT  DISTINCT
        a.claim_number,
        claim_lodgement_date,
        investigation_flag,
        denied_withdrawn_flag,
        contact_id,
        exposure_id,
        role_name,
        work_phone_number,
        home_phone_number,
        contact_mobile_number,
        mobile_phone_number,
        email_address_1,
        email_address_2,
        remittance_email,
        contact_first_name,
        contact_last_name,
        contact_full_name,
        contact_name,
        name,
        COALESCE(contact_mobile_number, mobile_phone_number, work_phone_number, home_phone_number) AS fixed_contact_number,
        -- COALESCE(contact_name, name, contact_full_name) AS fixed_contact_name,
        INITCAP(REGEXP_REPLACE(TRIM(COALESCE(NULLIF(contact_organisation_name, ''), name)), '\\s*\\(.*\\)\\s*$', '', 'g')) AS fixed_contact_name,
        COALESCE (contact_email_address_1, email_address_1, email_address_2, remittance_email) AS fixed_email_address,
        abn_number,
        bank_routing_number AS contact_bsb,
        bank_account_number AS contact_bank_account,
        bank_routing_number || bank_account_number AS contact_bank_full,
        address_line_1,
        address_line_2,
        address_suburb_name,
        address_post_code,
        address_state_name,
        address_line_1 || ' ' || address_line_2 ||' ' || address_suburb_name ||' ' || address_state_name  ||' ' ||  address_post_code AS full_address
FROM {{ ref('ctp_claim_features_dbt') }}  AS a
INNER JOIN ctx.mv_cc_ci_claim_contact_ext AS b
ON a.claim_number = b.claim_number
)

SELECT *
FROM cte_contact_full
WHERE fixed_contact_name !='Unknown Unknown'

-- SELECT  t1.claim_number AS claim_number_1, 
--     	t2.claim_number AS claim_number_2
    	
-- FROM 
--     cte_contact_full t1
-- JOIN 
--     cte_contact_full t2    
-- ON  
-- 	t1.fixed_email_address = t2.fixed_email_address
-- 	OR 
-- 	t1.full_address = t2.full_address
-- 	OR 
-- 	t1.fixed_contact_number = t2.fixed_contact_number
-- 	OR 
-- 	t1.contact_bank_full= t2.contact_bank_full
-- AND t1.claim_number <> t2.claim_number
