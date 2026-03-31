
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang
-- Description: shared pairs of doctor and lawyer data for building CTP networks
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION
-- 1.00     21/08/2024   Xiaomin Chang             Initial release
-- -------------------------------------------------------------------------

{{ config(
    materialized='table',
    distributed_by=['claim_number'],
    post_hook=grant_access(this)
) }}

WITH cte_vehicle_full AS (
        SELECT  a.claim_number, 
                vehicle_id, 
                vehicle_vin,
                vehicle_rego_number,	
                vehicle_make,
                vehicle_model,
                vehicle_registration_state_name,
                vehicle_year_of_manufacture,
                vehicle_driver_exists_flag,
                vehicle_owner_flag,
                vehicle_value_type_name
                incident_driver_first_name,
                incident_driver_last_name,
                incident_vehicle_loss_party_name
        FROM {{ ref('ctp_claim_features_dbt') }}  AS a
        JOIN ctx.mv_cc_ci_incident_driver_vehicle_ext b
        ON a.claim_number = b.claim_number
        JOIN pub.mv_fraud_investigations_cc_ci_ctp c
        ON c.claim_number = a.claim_number
        WHERE 	c.ctp_claim_type_name != 'IS - Inward Sharing'
    			AND LOWER(b.vehicle_rego_number) NOT IN ('unknown', 'unknow', 'uniden', 'unreg', 'noreg1', 'nds*11','XXXXXXXXXX')
)


SELECT  DISTINCT 
        a.claim_number AS claim_number_1,
        b.claim_number AS claim_number_2,
        a.vehicle_rego_number as rego_number
FROM cte_vehicle_full a
INNER JOIN cte_vehicle_full b
ON a.claim_number <> b.claim_number
AND (a.vehicle_vin = b.vehicle_vin
OR a.vehicle_rego_number = b.vehicle_rego_number)