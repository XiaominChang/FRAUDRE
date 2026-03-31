/* ============================================
   1) Vehicle at-fault (exposure-level; with rego)
   ============================================ */
DROP TABLE IF EXISTS ctp_vehicle_table;
CREATE TEMP TABLE ctp_vehicle_table AS
(
    SELECT
        a.claim_number,
        a.claim_loss_date,
        CAST(b.at_fault_claim_exposure_id AS INT) AS claim_exposure_id,
        b.at_fault_vehicle_id                     AS vehicle_id,
        c.vehicle_rego_number
    FROM ctx.mv_cc_ci_claim_header_ext AS a
    INNER JOIN pub_core.mv_ctp_vehicle_at_fault AS b
            ON a.claim_number = b.claim_number
    INNER JOIN pub.ctp_vehicle c
            ON b.claim_number       = c.claim_number
           AND b.at_fault_vehicle_id = c.vehicle_id
    WHERE a.claim_status_name <> 'Draft'
      AND a.ctp_statutory_insurer_state_name IN ('NSW')
      AND a.line_of_business_name = 'Compulsory Third Party'
      AND a.notify_only_claim_flag = 'No'
      AND (a.claim_closed_outcome_name IS NULL OR a.claim_closed_outcome_name = 'Completed')
      AND c.vehicle_rego_number IS NOT NULL
      AND LOWER(c.vehicle_rego_number) NOT IN ('unknown','unknow','uniden','unreg','noreg1','nds*11','xxxxxxxxxx')
      AND b.at_fault_claim_exposure_id IS NOT NULL
) DISTRIBUTED BY (claim_number);

/* ============================================
   2) Motor vehicle incidents (to align rego + loss date)
   ============================================ */
DROP TABLE IF EXISTS motor_vehicle_table;
CREATE TEMP TABLE motor_vehicle_table AS
(
    SELECT
        a.claim_number,
        a.claim_loss_date,
        b.vehicle_id,
        b.vehicle_rego_number
    FROM ctx.mv_cc_ci_claim_header_ext AS a
    INNER JOIN ctx.mv_cc_ci_incident_driver_vehicle_ext b
            ON a.claim_number = b.claim_number
    WHERE a.claim_loss_type_name = 'Motor'
      AND a.policy_issue_state   = 'NSW'
      AND a.notify_only_claim_flag = 'No'
      AND a.claim_closed_outcome_name NOT IN ('Duplicate','Open in error')
) DISTRIBUTED BY (claim_number);

/* ============================================
   3) Repairer link (join via rego + loss date)
   ============================================ */
DROP TABLE IF EXISTS repairer_claims;
CREATE TEMP TABLE repairer_claims AS
(
    SELECT DISTINCT
        a.claim_number,
        a.claim_exposure_id,
        a.vehicle_id,
        a.vehicle_rego_number,
        c.repairer_code,
        c.repairer_name_code,
        c.repairer_name_group,
        c.repairer_name
    FROM ctp_vehicle_table a
    INNER JOIN motor_vehicle_table b
            ON a.vehicle_rego_number = b.vehicle_rego_number
           AND a.claim_loss_date     = b.claim_loss_date
    INNER JOIN pub.mv_motor_assessing_monthly_historical c
            ON b.claim_number = c.claim_number
    WHERE c.repairer_relationship_type_code <> 'AMC'
) DISTRIBUTED BY (claim_number);


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


DROP TABLE IF EXISTS doctor_repairer_pairs;
CREATE TEMP TABLE doctor_repairer_pairs AS
(
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
DISTRIBUTED BY (claim_number);

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
AND a.repairer_name = b.repairer_name;
