
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: contact data for CTP networks building
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     14/08/2024   Xiaomin Chang             Initial release
-- -------------------------------------------------------------------------


WITH cte_contact AS (
    SELECT
        claim_number,
        contact_id,
        exposure_id,
        role_name,
        name,
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
        contact_date_of_birth_time,
        COALESCE(contact_name, name, contact_full_name) AS fixed_contact_name,
        COALESCE (contact_email_address_1, email_address_1, email_address_2) AS fixed_email_address,
        -- driving_license_number,
        -- passport_number,
        abn_number,
        bank_routing_number AS contact_bsb,
        bank_account_number AS contact_bank_account,
        bank_routing_number || bank_account_number AS contact_bank_full,
        address_line_1,
    	address_line_2,
        address_suburb_name,
        address_post_code,
        address_state_name,
        street_number,
        street_name
    FROM ctx.mv_cc_ci_claim_contact_ext      
)

SELECT *
FROM cte_contact