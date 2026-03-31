DROP TABLE IF EXISTS cte_claim;
CREATE TEMP TABLE cte_claim AS
(
    SELECT
      source_system,
      claim_number,
      claim_id,
      policy_number,
      policy_id AS policy_id_clm,
      policy_brand,
      policy_original_inception_date,
      policy_period_start_date,
      policy_period_end_date,
      policy_issue_state,
      policy_line_of_business_name,
      policy_system_type_code,
      policy_system_type_name,
      policy_issue_state_name,
      policy_status_name,
      vulnerable_customer_flag,
      loss_location_id,
      loss_suburb_name,
      claim_loss_state,
      claim_postcode,
      claim_status_name,
      claim_status_name_derived,
      claim_status_conformed,
      claim_closed_outcome_name,
      claim_decision_status_name,
      claimant_count,
      CAST(police_involved AS integer) AS police_flag,
      employee_claim_flag,
      fraud_risk_flag,
      section_one_score,
      refer_claim_to_investigations_team_name,
      investigation_outcome_name,
      fatality
    FROM ctx.mv_cc_ci_claim_header_ext
    WHERE claim_status_name <> 'Draft'
      AND ctp_statutory_insurer_state_name IN ('NSW')
      AND line_of_business_name = 'Compulsory Third Party'
      AND notify_only_claim_flag = 'No'
      AND claim_lodgement_date::date BETWEEN DATE '2018-01-01' AND CURRENT_DATE 
      AND (claim_closed_outcome_name IS NULL OR claim_closed_outcome_name = 'Completed')
)
DISTRIBUTED BY (claim_number);

DROP TABLE IF EXISTS cte_contact_full;
CREATE TEMP TABLE cte_contact_full AS
(
    SELECT DISTINCT
        a.claim_number,
        claim_lodgement_date,
        contact_id,
        b.exposure_id,
        c.claim_exposure_id,
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
        contact_organisation_name,
        COALESCE(contact_mobile_number, mobile_phone_number, work_phone_number, home_phone_number) AS fixed_contact_number,
        -- COALESCE(contact_name, name, contact_full_name) AS fixed_contact_name,
        INITCAP(REGEXP_REPLACE(
            TRIM(COALESCE(NULLIF(contact_organisation_name, ''), name)),
            '\\s*\\(.*\\)\\s*$',
            '',
            'g'
        )) AS fixed_contact_name,
        COALESCE(contact_email_address_1, email_address_1, email_address_2, remittance_email) AS fixed_email_address,
        abn_number,
        bank_routing_number AS contact_bsb,
        bank_account_number AS contact_bank_account,
        bank_routing_number || bank_account_number AS contact_bank_full,
        address_line_1,
        address_line_2,
        address_suburb_name,
        address_post_code,
        address_state_name,
        address_line_1 || ' ' || address_suburb_name || ' ' || address_state_name || ' ' || address_post_code AS full_address
    FROM cte_claim AS a
    INNER JOIN ctx.mv_cc_ci_claim_contact_ext AS b
      ON a.claim_number = b.claim_number
    INNER JOIN ctx.mv_cc_ci_claim_exposure_header_ext c
      ON a.claim_number = c.claim_number
     AND b.exposure_id = c.exposure_id
)
DISTRIBUTED BY (claim_number);

DROP TABLE IF EXISTS cte_contact_ctp;
CREATE TEMP TABLE cte_contact_ctp AS
(
    SELECT *
    FROM cte_contact_full
    WHERE fixed_contact_name <> 'Unknown Unknown'
      AND fixed_contact_name NOT IN (
        'Blacktown Hospital',
        'Gold Coast University Hospital And Health Service',
        'Goulburn Base Hospital',
        'Griffith Base Hospital',
        'John Hunter Hospital',
        'Lismore Base Hospital',
        'Liverpool Hospital',
        'Mt Druitt Hospital',
        'Nepean Hospital',
        'Nepean Private Hospital',
        'Norwest Private Hospital',
        'Port Macquarie Base Hospital',
        'Port Macquarie Private Hospital',
        'Prince Of Wales Hospital',
        'Royal North Shore Hospital',
        'Royal Prince Alfred Hospital',
        'St George Hospital',
        'St George Hospital And Community Health Service',
        'St George Hospital Community Health Servive',
        'St George Private Hospital',
        'St Vincents Hospital',
        'St Vincents Hospital Medical Imaging',
        'St Vincents Hospital Sydney',
        'St Vincents Hospital Sydney Ltd',
        'St Vincents Private Hospital',
        'Tamworth Hospital',
        'The Canberra Hospital',
        'The Childrens Hospital At Westmead',
        'The Prince Of Wales Hospital',
        'Wagga Base Hospital',
        'Westmead Hospital',
        'Westmead Private Hospital',
        'Wollongong Hospital',
        'Wollongong Hospital - Seslhd Host',
        'Wollongong Private Hospital',
        'Ethan Brooker',
        'Ethan Brooler'
      )
)
DISTRIBUTED BY (claim_number);

/* ============================================
   1) Doctor contacts (exposure-level)
   ============================================ */
DROP TABLE IF EXISTS doctor_claims;
CREATE TEMP TABLE doctor_claims AS
(
    SELECT DISTINCT
        claim_number,
        claim_exposure_id::INT AS claim_exposure_id,
        fixed_contact_number   AS doctor_contact_number,
        fixed_email_address    AS doctor_email_address,
        fixed_contact_name     AS doctor_contact_name,
        full_address           AS doctor_full_address
    FROM cte_contact_ctp
    WHERE role_name = 'Doctor'
)
DISTRIBUTED BY (claim_number);

/* ============================================
   2) Lawyer contacts (exposure-level)
   ============================================ */
DROP TABLE IF EXISTS lawyer_claims;
CREATE TEMP TABLE lawyer_claims AS
(
    SELECT DISTINCT
        claim_number,
        claim_exposure_id::INT AS claim_exposure_id,
        fixed_contact_number   AS lawyer_contact_number,
        fixed_email_address    AS lawyer_email_address,
        fixed_contact_name     AS lawyer_contact_name,
        full_address           AS lawyer_full_address
    FROM cte_contact_ctp
    WHERE role_name IN ('Legal', 'Legal Representative', 'Lawyer')
      AND fixed_contact_name NOT IN (
        'Hall and Wilcox','Hall & Wilcox','Hall And Wilcox Lawyers','Hall And Wilcox'
      )
)
DISTRIBUTED BY (claim_number);

/* ============================================
   3) Doctor-Lawyer pairs per exposure
   ============================================ */
DROP TABLE IF EXISTS doctor_lawyer_pairs;
CREATE TEMP TABLE doctor_lawyer_pairs AS
(
    SELECT DISTINCT
        a.claim_number,
        a.claim_exposure_id,
        /* Doctor */
        a.doctor_contact_number,
        a.doctor_email_address,
        a.doctor_contact_name,
        a.doctor_full_address,
        /* Lawyer */
        b.lawyer_contact_number,
        b.lawyer_email_address,
        b.lawyer_contact_name,
        b.lawyer_full_address
    FROM doctor_claims a
    JOIN lawyer_claims b
      ON a.claim_number      = b.claim_number
     AND a.claim_exposure_id = b.claim_exposure_id
)
DISTRIBUTED BY (claim_number);

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
FROM doctor_lawyer_pairs a
INNER JOIN doctor_lawyer_pairs b
  ON (a.claim_number <> b.claim_number OR a.claim_exposure_id <> b.claim_exposure_id)
 AND (
        a.doctor_contact_number = b.doctor_contact_number
     OR a.doctor_email_address  = b.doctor_email_address
     OR a.doctor_contact_name   = b.doctor_contact_name
     OR a.doctor_full_address   = b.doctor_full_address
     )
 AND (
        a.lawyer_contact_number = b.lawyer_contact_number
     OR a.lawyer_email_address  = b.lawyer_email_address
     OR a.lawyer_contact_name   = b.lawyer_contact_name
     OR a.lawyer_full_address   = b.lawyer_full_address
     );
