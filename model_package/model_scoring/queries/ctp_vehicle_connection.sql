
DROP TABLE IF EXISTS cte_vehicle_full;
CREATE TEMP TABLE cte_vehicle_full AS
(
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
	JOIN pub.mv_fraud_investigations_cc_ci_ctp d
    	ON a.claim_number = d.claim_number
	WHERE 	
	        claim_status_name <> 'Draft' 
		    AND ctp_statutory_insurer_state_name IN ('NSW')
		    AND a.line_of_business_name = 'Compulsory Third Party'
		    AND notify_only_claim_flag = 'No'
		    AND (claim_closed_outcome_name IS NULL OR claim_closed_outcome_name='Completed')
		    AND vehicle_rego_number IS NOT NULL 
		    AND LOWER(vehicle_rego_number) NOT IN ('unknown', 'unknow', 'uniden', 'unreg', 'noreg1', 'nds*11','XXXXXXXXXX')
		    AND b.at_fault_claim_exposure_id IS NOT NULL 
		    AND d.ctp_claim_type_name != 'IS - Inward Sharing'
		    AND a.claim_lodgement_date::date BETWEEN DATE '2018-01-01' AND CURRENT_DATE
	GROUP BY 
		    a.claim_number,
		    a.claim_loss_date,
		    b.at_fault_claim_exposure_id,
		    b.at_fault_vehicle_id,
		    c.vehicle_rego_number
) DISTRIBUTED BY (claim_number);

SELECT  DISTINCT 
	    a.claim_number AS claim_number_1,
	    a.claim_exposure_id AS claim_exposure_id_1,
	    b.claim_number AS claim_number_2,
	    b.claim_exposure_id AS claim_exposure_id_2,
        a.vehicle_rego_number as rego_number
FROM cte_vehicle_full a
INNER JOIN cte_vehicle_full b
ON (a.claim_number <> b.claim_number
OR a.claim_exposure_id <> b.claim_exposure_id)
AND a.vehicle_rego_number = b.vehicle_rego_number;
