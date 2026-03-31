
-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                               
-- Description: This table is used to extract features based CTP rules and  
-- customer data
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     08/03/2024      Xiaomin Chang             Initial release        
-- -------------------------------------------------------------------------

{{ config(
          materialized='table',
          distributed_by = ['policy_number'],
          post_hook = grant_access(this)
          ) }}

WITH hist_cust_policy AS (

  SELECT
    'SVX-' || svx_customer_key as svxr_customer_key,
    svx_customer_key,
    REGEXP_REPLACE(svx_policy_key, '^[A-Z]+-', '', 'g') AS svx_policy_key,
    policy_type_source,
    MAX(CASE WHEN(customer_policy_role = 'BILLING CLIENT') THEN 1 ELSE 0 END) AS billing_client_flag,
    MAX(CASE WHEN(customer_policy_role = 'DRIVER') THEN 1 ELSE 0 END) AS driver_flag,
    MAX(CASE WHEN(customer_policy_role = 'POLICY HOLDER') THEN 1 ELSE 0 END) AS policy_holder_flag,
    MAX(CASE WHEN(policy_status_code = 'CANCELLED') THEN 1 ELSE 0 END) AS cancelled_flag,
    MAX(CASE WHEN(policy_status_code = 'CURRENT') THEN 1 ELSE 0 END) AS current_flag,
    MAX(CASE WHEN(policy_status_code = 'LAPSED') THEN 1 ELSE 0 END) AS lapsed_flag,
    customer_policy_start_date,
    customer_policy_end_date,
    MIN(CASE WHEN(customer_policy_role = 'BILLING CLIENT') THEN customer_role_start_date ELSE NULL END) AS customer_BILLING_CLIENT_start_date,
    MAX(CASE WHEN(customer_policy_role = 'BILLING CLIENT') THEN customer_role_end_date ELSE NULL END) AS customer_BILLING_CLIENT_end_date,
    MIN(CASE WHEN(customer_policy_role = 'POLICY HOLDER') THEN customer_role_start_date ELSE NULL END) AS customer_POLICY_HOLDER_start_date,
    MAX(CASE WHEN(customer_policy_role = 'POLICY HOLDER') THEN customer_role_end_date ELSE NULL END) AS customer_POLICY_HOLDER_end_date,
    MIN(CASE WHEN(customer_policy_role = 'DRIVER') THEN customer_role_start_date ELSE NULL END) AS customer_DRIVER_start_date,
    MAX(CASE WHEN(customer_policy_role = 'DRIVER') THEN customer_role_end_date ELSE NULL END) AS customer_DRIVER_end_date
  FROM
    pub_restricted.mv_svx_iag_customer_policy a
  WHERE 
    customer_policy_start_date > '1950-01-01'   
    AND customer_policy_start_date <= '{{ var("end_date") }}'
  GROUP BY
    svx_customer_key,
    svx_policy_key,
    policy_type_source,
    customer_policy_start_date,
    customer_policy_end_date
),

-- customers involved in ctp claims   
customer_key_table_ctp AS ( 
  SELECT 
	   ch.policy_number, 
	   ch.claim_lodgement_date AS as_at_date,   
	   cp.svxr_customer_key,
	   cp.svx_customer_key 
  FROM  
    pub_core.mv_claim_header ch
  INNER JOIN hist_cust_policy cp    
    ON  ch.policy_number  = cp.svx_policy_key    
    AND cp.customer_policy_start_date <= ch.claim_loss_date
    AND cp.customer_policy_end_date >= ch.claim_loss_date
  WHERE ch.claim_loss_type_name = 'CTP'  AND ch.policy_issue_state='NSW' AND  ch.notify_only_claim_flag = 'No' 
        AND (ch.claim_closed_outcome_name IS NULL OR ch.claim_closed_outcome_name='Completed')
  GROUP BY  
    ch.policy_number,   
    ch.claim_lodgement_date,
    cp.svxr_customer_key,
    cp.svx_customer_key
),

-- customers involved in motor claims 
customer_key_table_motor AS ( 
  SELECT 
  	 ch.claim_number,
	   ch.policy_number, 
	   ch.claim_lodgement_date AS as_at_date,  
     ch.claim_loss_date, 
	   cp.svxr_customer_key,
	   cp.svx_customer_key 
  FROM  
    pub_core.mv_claim_header ch
  INNER JOIN hist_cust_policy cp    
    ON  ch.policy_number  = cp.svx_policy_key    
    AND cp.customer_policy_start_date <= ch.claim_loss_date
    AND cp.customer_policy_end_date >= ch.claim_loss_date
  WHERE ch.claim_loss_type_name = 'Motor' and ch.policy_issue_state='NSW' and ch.notify_only_claim_flag = 'No'
  GROUP BY  
    ch.claim_number,
    ch.policy_number,   
    ch.claim_lodgement_date,
    ch.claim_loss_date,
    cp.svxr_customer_key,
    cp.svx_customer_key
),


policy_key_table AS (
  select
    ckt.policy_number,
    ckt.svxr_customer_key,
    ckt.as_at_date,
    cp.svx_policy_key AS hist_policy_number,
    cp.customer_policy_start_date as hist_policy_start_date,
    cp.customer_policy_end_date as hist_policy_end_date,
    CASE WHEN cp.customer_policy_end_date < ckt.as_at_date AND policy_number != svx_policy_key THEN cp.cancelled_flag ELSE 0 END AS cancelled_flag,
    CASE WHEN cp.customer_policy_end_date < ckt.as_at_date AND policy_number != svx_policy_key THEN cp.lapsed_flag ELSE 0 END AS lapsed_flag,
    CASE WHEN cp.customer_policy_end_date >= ckt.as_at_date AND policy_number != svx_policy_key THEN 1 ELSE 0 END AS current_flag
  FROM
    customer_key_table_ctp ckt
  INNER JOIN hist_cust_policy cp
    ON ckt.svxr_customer_key = cp.svxr_customer_key
    AND cp.customer_policy_start_date <  ckt.as_at_date
  GROUP by
	    ckt.policy_number,
	    ckt.svxr_customer_key,
	    ckt.as_at_date,
	    cp.svx_policy_key,
	    cp.lapsed_flag,
	    cp.cancelled_flag,
	    cp.customer_policy_start_date,
	    cp.customer_policy_end_date
),

-- Relevant claim details WHERE claim was lodged before as at date
-- Includes one row per historical claim related to claim in question
claim_key_table AS (
  SELECT
    pkt.policy_number,
    pkt.as_at_date,
    pkt.svxr_customer_key,
    pkt.hist_policy_number,
    mch.claim_number AS hist_claim_number,
    mch.claim_loss_type_name AS hist_loss_type,
    mch.claim_lodgement_date AS hist_lodgement_date,
    mch.claim_loss_date AS hist_claim_loss_date
    
  FROM
    policy_key_table pkt
  INNER JOIN pub_core.mv_claim_header mch
    ON pkt.hist_policy_number = mch.policy_number
    AND mch.claim_lodgement_date < pkt.as_at_date
    AND pkt.hist_policy_start_date <= mch.claim_loss_date
    AND pkt.hist_policy_end_date >= mch.claim_loss_date
  GROUP BY
    pkt.policy_number,
    pkt.svxr_customer_key,
    pkt.as_at_date,
    pkt.hist_policy_number,
    mch.claim_number,
    mch.claim_loss_type_name,
    mch.claim_lodgement_date,
    mch.claim_loss_date
),

-- Unique list of historical customers involved in claims
customer_list as (
  SELECT DISTINCT
  svxr_customer_key,
  svx_customer_key
  FROM customer_key_table_ctp
),  

-- Unique list of historical policies related to claims
policy_list as (
  SELECT DISTINCT
  hist_policy_number
  FROM policy_key_table
),

-- Details relating to historical customers
-- One row per customer
customer_details AS (
  SELECT
    b.svxr_customer_key,
    b.svx_customer_key,
    MIN(customer_policy_start_date) AS customer_policy_start_date,
    MAX(customer_policy_end_date) AS customer_policy_end_date,
    MIN(customer_BILLING_CLIENT_start_date) AS customer_BILLING_CLIENT_start_date,
    MAX(customer_BILLING_CLIENT_end_date) AS customer_BILLING_CLIENT_end_date,
    MIN(customer_POLICY_HOLDER_start_date) AS customer_POLICY_HOLDER_start_date,
    MAX(customer_POLICY_HOLDER_end_date) AS customer_POLICY_HOLDER_end_date,
    MIN(customer_DRIVER_start_date) AS customer_DRIVER_start_date,
    MAX(customer_DRIVER_end_date) AS customer_DRIVER_end_date
  FROM
    customer_list a
  INNER JOIN hist_cust_policy b
    ON  a.svxr_customer_key = b.svxr_customer_key
  GROUP BY
    b.svxr_customer_key,
    b.svx_customer_key
 ),
 
-- Tenures for customers
-- One row per customer
hist_customer_view AS (
  SELECT
    a.policy_number,
    a.as_at_date,
    b.*,
    LEAST(customer_policy_end_date, a.as_at_date) - customer_policy_start_date AS customer_tenure,
    LEAST(CASE WHEN customer_BILLING_CLIENT_end_date IS NULL THEN NULL 
               WHEN customer_BILLING_CLIENT_end_date = '31/12/9999' THEN a.as_at_date 
               ELSE customer_BILLING_CLIENT_end_date END, a.as_at_date) - customer_BILLING_CLIENT_start_date AS customer_BILLING_CLIENT_tenure,
    LEAST(CASE WHEN customer_POLICY_HOLDER_end_date IS NULL THEN NULL 
               WHEN customer_POLICY_HOLDER_end_date = '31/12/9999' 
               THEN a.as_at_date 
               ELSE customer_POLICY_HOLDER_end_date END, a.as_at_date) - customer_POLICY_HOLDER_start_date AS customer_POLICY_HOLDER_tenure,
    LEAST(CASE WHEN customer_DRIVER_end_date IS NULL THEN NULL 
               WHEN customer_DRIVER_end_date = '31/12/9999' THEN a.as_at_date 
               ELSE customer_DRIVER_end_date END, a.as_at_date) - customer_DRIVER_start_date AS customer_DRIVER_tenure
  FROM
    customer_key_table_ctp a
  INNER JOIN customer_details b
    ON a.svxr_customer_key = b.svxr_customer_key
    AND b.customer_policy_start_date <= a.as_at_date
), 
    
-- Take summaries (max and min) of customers at as_at_date of interest
-- One row per as_at_date of interest
customer_summary AS (
  SELECT
    policy_number,
    as_at_date,
    COUNT(DISTINCT svxr_customer_key) AS cust_customer_count,
    MIN(customer_tenure) AS cust_min_customer_tenure,
    MAX(customer_tenure) AS cust_max_customer_tenure,
    AVG(customer_tenure) AS cust_mean_customer_tenure
  FROM
    hist_customer_view
  GROUP BY
    policy_number,
    as_at_date
),  

-- features related to historical claims
hist_claim_view AS (
  SELECT DISTINCT
    a.policy_number,
    a.as_at_date, -- lodgement date of current claim
    a.svxr_customer_key,
    a.hist_policy_number,
    b.claim_number AS hist_claim_number,
    CASE WHEN b.claim_number IS NOT NULL THEN 1
         ELSE 0
    END AS claim_count_total,
    
    CASE WHEN b.claim_number IS NOT NULL AND b.claim_loss_type_name = 'CTP' THEN 1
         ELSE 0
    END AS ctp_claim_count_total,
    
    CASE WHEN b.claim_number IS NOT NULL AND c.insured_flag=1 THEN 1
         ELSE 0
    END AS insured_claim_count_total,  
    CASE
        WHEN b.claim_loss_date BETWEEN DATE(a.as_at_date - INTERVAL '3 month') AND a.as_at_date THEN 1
        ELSE 0
    END AS claim_count_3_month,
    CASE
        WHEN b.claim_loss_date BETWEEN DATE(a.as_at_date - INTERVAL '6 month') AND a.as_at_date THEN 1
        ELSE 0
    END AS claim_count_6_month,
    CASE
        WHEN b.claim_loss_date BETWEEN DATE(a.as_at_date - INTERVAL '1 year') AND a.as_at_date THEN 1
        ELSE 0
    END AS claim_count_1yr,
    CASE
        WHEN b.claim_loss_date BETWEEN DATE(a.as_at_date - INTERVAL '2 year') AND a.as_at_date THEN 1
        ELSE 0
    END AS claim_count_2yr,
    CASE
        WHEN b.claim_loss_date BETWEEN DATE(a.as_at_date - INTERVAL '5 year') AND a.as_at_date THEN 1
        ELSE 0
    END AS claim_count_5yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '1 year') AND a.as_at_date) AND c.claim_total_loss_flag = 1 THEN 1
        ELSE 0
    END AS claim_total_loss_1yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '2 year') AND a.as_at_date) AND c.claim_total_loss_flag = 1 THEN 1
        ELSE 0
    END AS claim_total_loss_2yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '5 year') AND a.as_at_date) AND c.claim_total_loss_flag = 1 THEN 1
        ELSE 0
    END AS claim_total_loss_5yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '1 year') AND a.as_at_date) AND c.insured_total_loss_flag = 1 THEN 1
        ELSE 0
    END AS insured_total_loss_1yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '2 year') AND a.as_at_date) AND c.insured_total_loss_flag = 1 THEN 1
        ELSE 0
    END AS insured_total_loss_2yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '5 year') AND a.as_at_date) AND c.insured_total_loss_flag = 1 THEN 1
        ELSE 0
    END AS insured_total_loss_5yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '1 year') AND a.as_at_date) AND c.null_third_party_loss_flag = 1 THEN 1
        ELSE 0
    END AS non_thrid_party_total_loss_1yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '2 year') AND a.as_at_date) AND c.null_third_party_loss_flag = 1 THEN 1
        ELSE 0
    END AS non_thrid_party_total_loss_2yr,
    CASE
        WHEN (b.claim_loss_date BETWEEN date(a.as_at_date - INTERVAL '5 year') AND a.as_at_date) AND c.null_third_party_loss_flag = 1 THEN 1
        ELSE 0
    END AS non_thrid_party_total_loss_5yr,
    b.claim_lodgement_date AS hist_claim_lodgement_date,
    b.claim_loss_date AS hist_claim_loss_date,
    b.general_nature_of_loss_name_conformed AS hist_gnol,
    b.cause_of_loss_name AS hist_cause_of_loss,
    b.line_of_business_name_conformed AS hist_lob,
    b.fault_rating_name_conformed AS hist_fault_rating,
    b.total_excess_payable AS hist_total_excess,
    b.fraud_risk_flag AS hist_fraud_risk_flag,
    c.insured_flag AS insured_flag
  FROM
    policy_key_table a
  INNER JOIN pub_core.mv_claim_header b
    ON a.hist_policy_number = b.policy_number
    AND b.claim_lodgement_date < a.as_at_date
    AND a.hist_policy_start_date <= b.claim_lodgement_date
    AND a.hist_policy_end_date >= b.claim_lodgement_date
  LEFT JOIN 
    (
        SELECT distinct 
          claim_number
        , max(case when loss_party = 'insured' then 1 ELSE 0 end) over (partition by claim_number) as insured_flag
        , max(case when loss_party = 'insured' and total_loss_flag = 'Yes' then 1 ELSE 0 end) over (partition by claim_number) as insured_total_loss_flag
        , max(case when total_loss_flag = 'Yes' then 1 ELSE 0 end) over (partition by claim_number) as claim_total_loss_flag
        , max(case when loss_party is null and claim_loss_type_name = 'CTP'  then 1 ELSE 0 end) over (partition by claim_number) as null_third_party_loss_flag
        FROM ctx.mv_cc_ci_claim_exposure_header_ext mcccehe 
        WHERE claim_loss_type_name = 'Motor' or claim_loss_type_name = 'CTP'
    ) c
    ON 
        b.claim_number = c.claim_number
), 


claims_summary as (
  select
  	policy_number,
    as_at_date,
    svxr_customer_key,
    SUM(claim_count_total) AS claim_count_total,
    SUM(ctp_claim_count_total) AS ctp_claim_count_total,
    SUM(insured_claim_count_total) AS insured_claim_count_total,
    SUM(claim_count_3_month) AS claim_count_3_month,
    SUM(claim_count_6_month) AS claim_count_6_month,
    SUM(claim_count_1yr) AS claim_count_1yr,
    SUM(claim_count_2yr) AS claim_count_2yr,
    SUM(claim_count_5yr) AS claim_count_5yr,      
    SUM(CASE WHEN hist_lob = 'Personal Motor' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_motor,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_collision,
    
    SUM(CASE WHEN hist_lob = 'Personal Motor' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_motor,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_collision,
    
    SUM(CASE WHEN hist_lob = 'Personal Motor' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_motor,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_collision,
    
    
    SUM(CASE WHEN hist_lob = 'Personal Motor' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_motor,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_collision,

    SUM(CASE WHEN hist_lob = 'Personal Motor' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_motor,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_collision,
    
    
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Insured at fault' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_insured_fault_ctp,   
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Fault unknown' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_unknown_fault_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Third party at fault' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_insured_third_party_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss ilike'%pedestrian%' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_pedestrian_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss IN ( '20 Head on',	'30 Rear end', '62 Accident', '67 Struck animal')
		THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_multi_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss = '99 Unknown' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_cause_unknown_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Insured at fault' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_insured_fault,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Third party at fault' THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_insured_third_party,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_cause_of_loss IN ('Hit Animal - Recovery',    																
                                                                     'Hit Animal - No Recovery',
                                                                     'Hit Animal - No Recovery',
                                                                     'Hit Animal - No Recovery',                                                                     
                                                                     'Hit Stationary Object',
                                                                     'Single Vehicle Accident',
                                                                     'Hit Pedestrian / Cyclist') THEN claim_count_3_month ELSE 0 END) AS claim_count_3_month_sva,
                                                                     
                                                                     
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Insured at fault' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_insured_fault_ctp,   
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Fault unknown' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_unknown_fault_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Third party at fault' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_insured_third_party_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss ilike'%pedestrian%' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_pedestrian_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss IN ( '20 Head on',	'30 Rear end', '62 Accident', '67 Struck animal')
		THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_multi_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss = '99 Unknown' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_cause_unknown_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Insured at fault' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_insured_fault,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Third party at fault' THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_insured_third_party,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_cause_of_loss IN ('Hit Animal - Recovery', 
                                                                     'Hit Animal - No Recovery',
                                                                     'Hit Stationary Object',
                                                                     'Single Vehicle Accident',
                                                                     'Hit Pedestrian / Cyclist') THEN claim_count_6_month ELSE 0 END) AS claim_count_6_month_sva,
    
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Insured at fault' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_insured_fault_ctp,   
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Fault unknown' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_unknown_fault_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Third party at fault' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_insured_third_party_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss ilike'%pedestrian%' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_pedestrian_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss IN ( '20 Head on',	'30 Rear end', '62 Accident', '67 Struck animal')
		THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_multi_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss = '99 Unknown' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_cause_unknown_ctp,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Insured at fault' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_insured_fault,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Third party at fault' THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_insured_third_party,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_cause_of_loss IN ('Hit Animal - Recovery', 
                                                                     'Hit Animal - No Recovery',
                                                                     'Hit Stationary Object',
                                                                     'Single Vehicle Accident',
                                                                     'Hit Pedestrian / Cyclist') THEN claim_count_1yr ELSE 0 END) AS claim_count_1yr_sva,
    
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Insured at fault' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_insured_fault_ctp,   
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Fault unknown' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_unknown_fault_ctp,  
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Third party at fault' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_insured_third_party_ctp,    
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss ilike'%pedestrian%' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_pedestrian_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss IN ( '20 Head on',	'30 Rear end', '62 Accident', '67 Struck animal')
		THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_multi_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss = '99 Unknown' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_cause_unknown_ctp,   
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Insured at fault' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_insured_fault,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Third party at fault' THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_insured_third_party,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_cause_of_loss IN ('Hit Animal - Recovery', 
                                                                     'Hit Animal - No Recovery',
                                                                     'Hit Stationary Object',
                                                                     'Single Vehicle Accident',
                                                                     'Hit Pedestrian / Cyclist') THEN claim_count_2yr ELSE 0 END) AS claim_count_2yr_sva,
                                                                     
                                                                     
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Insured at fault' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_insured_fault_ctp,   
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Fault unknown' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_unknown_fault_ctp,                                                                     
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' AND hist_fault_rating = 'Third party at fault' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_insured_third_party_ctp,
    SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss ilike'%pedestrian%' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_pedestrian_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss IN ( '20 Head on',	'30 Rear end', '62 Accident', '67 Struck animal')
		THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_multi_ctp,
	  SUM(CASE WHEN hist_lob = 'Compulsory Third Party' and hist_cause_of_loss = '99 Unknown' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_cause_unknown_ctp,   
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Insured at fault' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_insured_fault,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_fault_rating = 'Third party at fault' THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_insured_third_party,
    SUM(CASE WHEN hist_gnol = 'Collision' AND hist_cause_of_loss IN ('Hit Animal - Recovery', 
                                                                     'Hit Animal - No Recovery',
                                                                     'Hit Stationary Object',
                                                                     'Single Vehicle Accident',
                                                                     'Hit Pedestrian / Cyclist') THEN claim_count_5yr ELSE 0 END) AS claim_count_5yr_sva,
                                                                                                                                                                                                                                                                                                                                                                                                                                                         
    SUM(claim_total_loss_1yr) AS claim_total_loss_1yr,
    SUM(claim_total_loss_2yr) AS claim_total_loss_2yr,
    SUM(claim_total_loss_5yr) AS claim_total_loss_5yr,
    SUM(insured_total_loss_1yr) AS insured_total_loss_1yr,
    SUM(insured_total_loss_2yr) AS insured_total_loss_2yr,
    SUM(insured_total_loss_5yr) AS insured_total_loss_5yr                                                                 
  FROM
    hist_claim_view
  GROUP by
  	policy_number,
    as_at_date,
    svxr_customer_key
),

claims_summary_cust AS (
  select
  	policy_number,
    as_at_date,
    SUM(claim_count_total) AS sum_claim_count_total,
    SUM(ctp_claim_count_total) AS sum_ctp_claim_count_total,
    MAX(claim_count_total) AS max_claim_count_total,
    MAX(ctp_claim_count_total) AS max_ctp_claim_count_total,
    MAX(insured_claim_count_total) AS max_insured_claim_count_total,
    MAX(claim_count_3_month) AS max_claim_count_3_month,
    MAX(claim_count_6_month) AS max_claim_count_6_month,    
    MAX(claim_count_1yr) AS max_claim_count_1yr,
    MAX(claim_count_2yr) AS max_claim_count_2yr,
    MAX(claim_count_5yr) AS max_claim_count_5yr,
   
    MAX(claim_count_3_month_motor) AS max_claim_count_3_month_motor,
    MAX(claim_count_3_month_ctp) AS max_claim_count_3_month_ctp,
    MAX(claim_count_3_month_collision) AS max_claim_count_3_month_collision,
    MAX(claim_count_3_month_insured_fault_ctp) AS max_claim_count_3_month_insured_fault_ctp,
    MAX(claim_count_3_month_unknown_fault_ctp) AS max_claim_count_3_month_unknown_fault_ctp,
    MAX(claim_count_3_month_insured_third_party_ctp) AS max_claim_count_3_month_insured_third_party_ctp,
    MAX(claim_count_3_month_pedestrian_ctp) AS max_claim_count_3_month_pedestrian_ctp,
    MAX(claim_count_3_month_multi_ctp) AS max_claim_count_3_month_multi_ctp,
    MAX(claim_count_3_month_cause_unknown_ctp) AS max_claim_count_3_month_cause_unknown_ctp,                
    MAX(claim_count_3_month_insured_fault) AS max_claim_count_3_month_insured_fault,
    MAX(claim_count_3_month_insured_third_party) AS max_claim_count_3_month_insured_third_party,
    MAX(claim_count_3_month_sva) AS max_claim_count_3_month_sva,
    
    
    
    MAX(claim_count_6_month_motor) AS max_claim_count_6_month_motor,
    MAX(claim_count_6_month_ctp) AS max_claim_count_6_month_ctp,
    MAX(claim_count_6_month_collision) AS max_claim_count_6_month_collision,
    MAX(claim_count_6_month_insured_fault_ctp) AS max_claim_count_6_month_insured_fault_ctp,
    MAX(claim_count_6_month_unknown_fault_ctp) AS max_claim_count_6_month_unknown_fault_ctp,
    MAX(claim_count_6_month_insured_third_party_ctp) AS max_claim_count_6_month_insured_third_party_ctp,
    MAX(claim_count_6_month_pedestrian_ctp) AS max_claim_count_6_month_pedestrian_ctp,
    MAX(claim_count_6_month_multi_ctp) AS max_claim_count_6_month_multi_ctp,
    MAX(claim_count_6_month_cause_unknown_ctp) AS max_claim_count_6_month_cause_unknown_ctp,                
    MAX(claim_count_6_month_insured_fault) AS max_claim_count_6_month_insured_fault,
    MAX(claim_count_6_month_insured_third_party) AS max_claim_count_6_month_insured_third_party,
    MAX(claim_count_6_month_sva) AS max_claim_count_6_month_sva,
    
    MAX(claim_count_1yr_motor) AS max_claim_count_1yr_motor,
    MAX(claim_count_1yr_ctp) AS max_claim_count_1yr_ctp,
    MAX(claim_count_1yr_collision) AS max_claim_count_1yr_collision,
    MAX(claim_count_1yr_insured_fault_ctp) AS max_claim_count_1yr_insured_fault_ctp,
    MAX(claim_count_1yr_unknown_fault_ctp) AS max_claim_count_1yr_unknown_fault_ctp,
    MAX(claim_count_1yr_insured_third_party_ctp) AS max_claim_count_1yr_insured_third_party_ctp,
    MAX(claim_count_1yr_pedestrian_ctp) AS max_claim_count_1yr_pedestrian_ctp,
    MAX(claim_count_1yr_multi_ctp) AS max_claim_count_1yr_multi_ctp,
    MAX(claim_count_1yr_cause_unknown_ctp) AS max_claim_count_1yr_cause_unknown_ctp,                
    MAX(claim_count_1yr_insured_fault) AS max_claim_count_1yr_insured_fault,
    MAX(claim_count_1yr_insured_third_party) AS max_claim_count_1yr_insured_third_party,
    MAX(claim_count_1yr_sva) AS max_claim_count_1yr_sva,
    
    MAX(claim_count_2yr_motor) AS max_claim_count_2yr_motor,
    MAX(claim_count_2yr_ctp) AS max_claim_count_2yr_ctp,
    MAX(claim_count_2yr_collision) AS max_claim_count_2yr_collision,
    MAX(claim_count_2yr_insured_fault_ctp) AS max_claim_count_2yr_insured_fault_ctp,
    MAX(claim_count_2yr_unknown_fault_ctp) AS max_claim_count_2yr_unknown_fault_ctp,
    MAX(claim_count_2yr_insured_third_party_ctp) AS max_claim_count_2yr_insured_third_party_ctp,
    MAX(claim_count_2yr_pedestrian_ctp) AS max_claim_count_2yr_pedestrian_ctp,
    MAX(claim_count_2yr_multi_ctp) AS max_claim_count_2yr_multi_ctp,
    MAX(claim_count_2yr_cause_unknown_ctp) AS max_claim_count_2yr_cause_unknown_ctp,                
    MAX(claim_count_2yr_insured_fault) AS max_claim_count_2yr_insured_fault,
    MAX(claim_count_2yr_insured_third_party) AS max_claim_count_2yr_insured_third_party,
    MAX(claim_count_2yr_sva) AS max_claim_count_2yr_sva,
    
    MAX(claim_count_5yr_motor) AS max_claim_count_5yr_motor,
    MAX(claim_count_5yr_ctp) AS max_claim_count_5yr_ctp,
    MAX(claim_count_5yr_collision) AS max_claim_count_5yr_collision,
    MAX(claim_count_5yr_insured_fault_ctp) AS max_claim_count_5yr_insured_fault_ctp,
    MAX(claim_count_5yr_unknown_fault_ctp) AS max_claim_count_5yr_unknown_fault_ctp,
    MAX(claim_count_5yr_insured_third_party_ctp) AS max_claim_count_5yr_insured_third_party_ctp,
    MAX(claim_count_5yr_pedestrian_ctp) AS max_claim_count_5yr_pedestrian_ctp,
    MAX(claim_count_5yr_multi_ctp) AS max_claim_count_5yr_multi_ctp,
    MAX(claim_count_5yr_cause_unknown_ctp) AS max_claim_count_5yr_cause_unknown_ctp,                
    MAX(claim_count_5yr_insured_fault) AS max_claim_count_5yr_insured_fault,
    MAX(claim_count_5yr_insured_third_party) AS max_claim_count_5yr_insured_third_party,
    MAX(claim_count_5yr_sva) AS max_claim_count_5yr_sva,
    
    MAX(claim_total_loss_1yr) as max_claim_total_loss_1yr,
    MAX(claim_total_loss_2yr) as max_claim_total_loss_2yr,
    MAX(claim_total_loss_5yr) as max_claim_total_loss_5yr,
    MAX(insured_total_loss_1yr) as max_insured_total_loss_1yr,
    MAX(insured_total_loss_2yr) as max_insured_total_loss_2yr,
    MAX(insured_total_loss_5yr) as max_insured_total_loss_5yr
  FROM
    claims_summary
--  WHERE ctp_claim_count_total <>0
  GROUP by
  	policy_number,
    as_at_date
),


-- Define investigation flags of historical claims
-- One row per historical claim
inv_flag AS (
  SELECT DISTINCT 
    a.claim_number,
    MAX(CASE WHEN investigation_activily_level = 'Investigated' AND activity_create_time < as_at_date THEN 1 ELSE 0 END) OVER (PARTITION BY a.claim_number) AS investigation_flag,
    MIN(CASE WHEN investigation_activily_level = 'Investigated' AND activity_create_time < as_at_date THEN as_at_date - activity_create_time ELSE NULL END) OVER (PARTITION BY a.claim_number) AS last_investigation_days
  FROM
    pub.mv_fraud_investigations_cc_ci_inv_db_activities a
  INNER JOIN hist_claim_view b
    ON a.claim_number = b.hist_claim_number
) ,

-- Define denied withdrawn flags of historical claims
-- One row per historical claim
inv_denied AS (
  SELECT 
    a.claim_number,
    max(CASE WHEN denied_withdrawn_flag = 1 AND activity_create_time < as_at_date THEN 1 ELSE 0 END) AS denied_withdrawn_flag
  FROM
    pub.mv_fraud_investigations_cc_ci_inv_db_activities a
  INNER JOIN hist_claim_view b
     ON a.claim_number = b.hist_claim_number
  WHERE
    investigation_outcome_name IS NOT NULL
  GROUP BY a.claim_number
), 


-- Define manual referrals and system alerts of historical claims
-- One row per historical claim
fraud_summary AS (
  SELECT DISTINCT
    a.claim_number,
    CASE WHEN manual_referral_flag = 1 AND initial_activity_time < as_at_date THEN 1 ELSE 0 END AS manual_referral_flag,
    CASE WHEN manual_referral_flag = 1 AND initial_activity_time < as_at_date THEN as_at_date - initial_activity_time ELSE NULL END AS last_manual_referral_days,
    LEAST(CASE WHEN auto_alert_flag = 1 AND initial_activity_time < as_at_date THEN 1 ELSE 0 END + CASE WHEN faa_alert_flag = 1 AND initial_activity_time < as_at_date THEN 1 ELSE 0 END, 1) as system_alert_flag,
    CASE WHEN a.investigation_status !='Closed' AND initial_activity_time < as_at_date THEN 1 ELSE 0 END AS under_investigation_flag 
  FROM
    pub.mv_fraud_investigations_summarised a
  INNER JOIN hist_claim_view b 
      ON a.claim_number = b.hist_claim_number
), 
--

---- Summarise F&I counts

 investigation_summary AS (
  select
    a.policy_number,
    a.as_at_date,
    a.svxr_customer_key,
    SUM(b.investigation_flag) AS investigation_count,
    SUM(c.denied_withdrawn_flag) AS denial_count,
    SUM(d.system_alert_flag) AS system_alert_count,
    SUM(d.manual_referral_flag) AS manual_referral_count,
    SUM(d.under_investigation_flag) AS under_investigation_count,
    MIN(b.last_investigation_days) AS last_investigation_days,
    MIN(d.last_manual_referral_days) AS last_manual_referral_days
  FROM
    hist_claim_view a
  LEFT JOIN inv_flag b 
    ON a.hist_claim_number = b.claim_number
  LEFT JOIN inv_denied c 
    ON a.hist_claim_number = c.claim_number
  LEFT JOIN fraud_summary d 
    ON a.hist_claim_number = d.claim_number
  GROUP by
    a.policy_number,
    a.as_at_date,
    a.svxr_customer_key
),


-- Take summaries (max and min) of historical F&I activity involved with customer at as_at_date of interest
-- One row per as_at_date of interest
 investigation_summary_cust AS (
  select
    a.policy_number,
    a.as_at_date,
    MAX(investigation_count) AS max_investigation_count,
    MAX(denial_count) AS max_denial_count,
    MAX(system_alert_count) AS max_system_alert_count,
    MAX(manual_referral_count) AS max_manual_referral_count,
    MAX(under_investigation_count) AS max_under_investigation_count,
    MIN(last_investigation_days) AS min_last_investigation_days,
    MIN(last_manual_referral_days) AS min_last_manual_referral_days
  FROM
    investigation_summary a
  GROUP by
    a.policy_number,
    a.as_at_date 
 ),
 
 policy_arrears_summary AS (
 SELECT
    a.policy_number,
    a.as_at_date,
     a.svxr_customer_key,
     SUM(CASE WHEN (b.arrears_type is not null) THEN 1 ELSE 0 END) AS number_cancel_arrear_3yr,
     SUM(CASE WHEN (b.arrears_type = 'Motor') THEN 1 ELSE 0 END) AS number_cancel_arrear_3yr_motor,
     SUM(CASE WHEN (b.arrears_type = 'Home') THEN 1 ELSE 0 END) AS number_cancel_arrear_3yr_home,
     SUM(CASE WHEN (b.arrears_type = 'CTP') THEN 1 ELSE 0 END) AS number_cancel_arrear_3yr_ctp,
     MIN(a.as_at_date - b.policy_effective_FROM_date) AS last_cancel_arrear_days
 FROM
    policy_key_table a
 LEFT JOIN 
     (SELECT DISTINCT a.policy_number, 
                      TO_DATE(effective_FROM_date::CHARACTER VARYING(8), 'yyyymmdd') AS policy_effective_FROM_date, 
                      'Motor' AS arrears_type 
      FROM ctx.mv_huon_pi_policy_transaction_motor_extn b
      INNER JOIN policy_key_table a
       ON a.hist_policy_number = b.policy_number_extended
       AND to_date(b.effective_FROM_date::CHARACTER VARYING(8), 'yyyymmdd') < a.as_at_date
      WHERE transaction_type_code IN ('0300', '0310') AND lapse_cancel_reason_code = 97
     
      UNION
      
      SELECT DISTINCT a.policy_number, 
                      TO_DATE(effective_FROM_date::CHARACTER VARYING(8), 'yyyymmdd') AS policy_effective_FROM_date, 
                      'Home' AS arrears_type 
      FROM ctx.mv_huon_pi_policy_transaction_home_extn b
      INNER JOIN policy_key_table a
       ON a.hist_policy_number = b.policy_number_extended
       AND to_date(b.effective_FROM_date::CHARACTER VARYING(8), 'yyyymmdd') < a.as_at_date
      WHERE transaction_type_code IN ('0300', '0310') AND lapse_cancel_reason_code = 97
      
      UNION
      
      SELECT DISTINCT a.policy_number, 
                      TO_DATE(effective_FROM_date::CHARACTER VARYING(8), 'yyyymmdd') AS policy_effective_FROM_date, 
                      'CTP' AS arrears_type
      FROM ctx.mv_huon_pi_policy_transaction_ctp_extn b
      INNER JOIN policy_key_table a
       ON a.hist_policy_number = b.policy_number_extended
       AND to_date(b.effective_FROM_date::CHARACTER VARYING(8), 'yyyymmdd') < a.as_at_date
      WHERE transaction_type_code IN ('0300', '0310') AND lapse_cancel_reason_code = 97
      
      UNION
     
      SELECT DISTINCT a.policy_number, 
                      TO_DATE(policy_period_edit_effective_date::character VARYING(8), 'yyyymmdd') AS policy_effective_FROM_date,
                      CASE WHEN product_name IN ('Car Insurance', 'Caravan Insurance', 'Trailer Insurance', 'Motorcycle Insurance') THEN 'Motor'
                           WHEN product_name IN ('Home Insurance', 'Landlord Insurance') THEN 'Home' END AS arrears_type
      FROM consumption.tb_policy_period_common b
      INNER JOIN policy_key_table a
      ON a.hist_policy_number = b.policy_number
      AND to_date(policy_period_edit_effective_date::character VARYING(8), 'yyyymmdd') < a.as_at_date
      WHERE job_code = 'Cancellation' AND cancellation_reason_name = 'Non-payment' AND policy_status_name = 'Bound'
      AND product_name IN ('Car Insurance', 'Caravan Insurance', 'Trailer Insurance', 'Motorcycle Insurance','Home Insurance', 'Landlord Insurance')
     ) b
  ON a.hist_policy_number = b.policy_number
  AND (b.policy_effective_FROM_date < a.as_at_date) 
  AND (b.policy_effective_FROM_date >= (a.as_at_date - 1095))
 GROUP BY a.policy_number,
          a.as_at_date,
          a.svxr_customer_key
), 

-- Take summaries (max and min) of policies arrears for customers involved at as_at_date of interest
-- One row per as_at_date of interest
policy_arrears_summary_cust AS (
 SELECT
    policy_number,
    as_at_date,
     MAX(number_cancel_arrear_3yr) AS max_number_cancel_arrear_3yr,
     MAX(number_cancel_arrear_3yr_motor) AS max_number_cancel_arrear_3yr_motor,
     MAX(number_cancel_arrear_3yr_home) AS max_number_cancel_arrear_3yr_home,
     MAX(number_cancel_arrear_3yr_ctp) AS max_number_cancel_arrear_3yr_ctp,
     MIN(last_cancel_arrear_days) AS min_last_cancel_arrear_days
 FROM policy_arrears_summary
 GROUP BY policy_number, 
          as_at_date
),
 
 
 -- Summarise policy features
-- One row per customer
policy_summary AS (
  SELECT
    policy_number,
    as_at_date,
    count(*) AS policy_count,
    SUM(CASE WHEN current_flag = 1 THEN 1 ELSE 0 END) + 1 AS active_policies_count, -- +1 is policy with claim
    SUM(CASE WHEN cancelled_flag = 1 THEN 1 ELSE 0 END) AS cancel_policies_count,
    SUM(CASE WHEN lapsed_flag = 1 THEN 1 ELSE 0 END) AS lapse_policies_count
  FROM
    policy_key_table
  GROUP BY
    policy_number,
    as_at_date,
    svxr_customer_key
),

-- Take summaries (max and min) of historical policies involved with customer at as_at_date of interest
-- One row per as_at_date of interest
policy_summary_cust AS (
  SELECT
    policy_number,
    as_at_date,
    MAX(policy_count) AS max_policy_count,
    MAX(active_policies_count) AS max_active_policies_count,
    MAX(cancel_policies_count) AS max_cancel_policies_count,
    MAX(lapse_policies_count) AS max_lapse_policies_count
  FROM
    policy_summary
  GROUP BY
    policy_number,
    as_at_date
),

-- Summarise complaints relating to customer
-- Duplication in complaints due to priority elevation (level 0, level 1 etc)
-- So we take the complaint level that has happened most recently and take that row
-- One row per customer
complaint_summary AS (
  SELECT 
    policy_number,
    as_at_date,
    svxr_customer_key,
    COUNT(*) as num_complaints_3yr,
    SUM(CASE WHEN complaint_level = 'Level 0' THEN 1 ELSE 0 END) AS level_0_complaints_3yr,
    SUM(CASE WHEN complaint_level = 'Level 1' THEN 1 ELSE 0 END) AS level_1_complaints_3yr,
    SUM(CASE WHEN complaint_level = 'Level 2' THEN 1 ELSE 0 END) AS level_2_complaints_3yr,
    SUM(CASE WHEN complaint_level = 'Level 3' THEN 1 ELSE 0 END) AS level_3_complaints_3yr,
    MIN(as_at_date - complaint_received_date) AS last_complaint_days,
    SUM(CASE WHEN harmozined_touchpoint = 'Sales & Service' THEN 1 ELSE 0 END) AS sales_complaints_3yr,
    SUM(CASE WHEN harmozined_touchpoint = 'Claims & Assessing' THEN 1 ELSE 0 END) AS claims_complaints_3yr,
    SUM(CASE WHEN harmonized_outcome IN ('Decision Maintained', 'Withdrawn by Customer') THEN 1 ELSE 0 END) AS iag_favour_complaints_3yr,
    SUM(CASE WHEN harmonized_outcome = 'In favour of Customer' THEN 1 ELSE 0 END) AS customer_favour_complaints_3yr,
    SUM(CASE WHEN harmonized_outcome = 'Outstanding' THEN 1 ELSE 0 END) as outstanding_complaints_3yr,
    SUM(CASE WHEN complaint_category_1 IN ('Rejection', 'Claim denied (in full)', 'Out Of Cover') THEN 1 ELSE 0 END) AS claim_rejection_complaints_3yr
    FROM (SELECT DISTINCT policy_number, 
                          as_at_date, 
                          b.svxr_customer_key, 
                          source_system_complaint_id, 
                          FIRST_VALUE(harmozined_touchpoint) OVER (PARTITION by policy_number, as_at_date, b.svx_customer_key, source_system_complaint_id ORDER BY complaint_finalised_date DESC NULLS LAST) AS harmozined_touchpoint,
                          FIRST_VALUE(harmonized_outcome) OVER (PARTITION by policy_number, as_at_date, b.svx_customer_key, source_system_complaint_id ORDER BY complaint_finalised_date DESC NULLS LAST) AS harmonized_outcome,
                          FIRST_VALUE(complaint_category_1) OVER (PARTITION by policy_number, as_at_date, b.svx_customer_key, source_system_complaint_id ORDER BY complaint_finalised_date DESC NULLS LAST) AS complaint_category_1,
                          FIRST_VALUE(complaint_level) OVER (PARTITION by policy_number, as_at_date, b.svx_customer_key, source_system_complaint_id ORDER BY complaint_finalised_date DESC NULLS LAST) AS complaint_level,
                          FIRST_VALUE(complaint_received_date) OVER (PARTITION by policy_number, as_at_date, b.svx_customer_key, source_system_complaint_id ORDER BY complaint_finalised_date DESC NULLS LAST) AS complaint_received_date
          FROM pub_restricted.mv_svx_iag_customer_activity_complaints a
          INNER JOIN customer_key_table_ctp b
            ON a.svx_customer_key = b.svx_customer_key
            AND a.complaint_received_date < b.as_at_date
            AND a.complaint_received_date >= b.as_at_date - 1095
         ) foo
    GROUP BY policy_number,
             as_at_date,
             svxr_customer_key
),

-- Take summaries (max and min) of historical complaint activity involved with customer at as_at_date of interest
-- One row per as_at_date of interest
complaint_summary_cust AS (
  SELECT 
    policy_number,
    as_at_date,
    MAX(num_complaints_3yr) AS max_num_complaints_3yr,
    MAX(level_0_complaints_3yr) AS max_level_0_complaints_3yr,
    MAX(level_1_complaints_3yr) AS max_level_1_complaints_3yr,
    MAX(level_2_complaints_3yr) AS max_level_2_complaints_3yr,
    MAX(level_3_complaints_3yr) AS max_level_3_complaints_3yr,
    MIN(last_complaint_days) AS min_last_complaint_days,
    MAX(sales_complaints_3yr) AS max_sales_complaints_3yr,
    MAX(claims_complaints_3yr) AS max_claims_complaints_3yr,
    MAX(iag_favour_complaints_3yr) AS max_iag_favour_complaints_3yr,
    MAX(customer_favour_complaints_3yr) AS max_customer_favour_complaints_3yr,
    MAX(outstanding_complaints_3yr) AS max_outstanding_complaints_3yr,
    MAX(claim_rejection_complaints_3yr) AS max_claim_rejection_complaints_3yr
    FROM complaint_summary
    GROUP BY policy_number,
             as_at_date
     ),

-- Find interactions relating to customer
-- Assigning the type of interaction and the time period associated with that interaction
-- One row per customer interaction
interactions AS (
  SELECT DISTINCT
   policy_number,
   as_at_date,
   b.svx_customer_key,
   contact_id,
   interaction_channel_name,
   duration,
   customer_contact_start_date,
   CASE WHEN customer_contact_start_date BETWEEN date(as_at_date - INTERVAL '1 month') AND as_at_date THEN 1 ELSE 0 END AS interaction_1month,
   CASE WHEN customer_contact_start_date BETWEEN date(as_at_date - INTERVAL '3 month') AND as_at_date THEN 1 ELSE 0 END AS interaction_3month,
   CASE WHEN customer_contact_start_date BETWEEN date(as_at_date - INTERVAL '6 month') AND as_at_date THEN 1 ELSE 0 END AS interaction_6month,
   CASE WHEN interaction_channel_name IN ('WebInteractionED', 'MobileAppInteractionED') THEN 'Web'
        WHEN interaction_channel_name IN ('EmailED', 'TextChatInteractionED', 'SocialMessageED', 'SMSED') THEN 'Text'
        WHEN interaction_channel_name = 'TelephoneCallED' THEN 'Tele'
        WHEN interaction_channel_name = 'Face2FaceED' THEN 'F2F' 
        ELSE 'Other' END AS int_type
  FROM pub_restricted.mv_svx_iag_customer_activity_kana a
  RIGHT JOIN customer_key_table_ctp b
     ON a.svx_customer_key = b.svx_customer_key::int
     AND a.customer_contact_start_date < b.as_at_date
),
 
-- Interaction summary which has aggregate counts, last interaction and the duration of last interaction
-- One row per customer
interactions_summary AS (
  SELECT 
       a.*, 
       b.web_latest_duration, 
       b.tele_latest_duration
  FROM (
  SELECT
   policy_number,
   as_at_date,
   svx_customer_key,
   SUM(interaction_1month) AS customer_interactions_1month,
   SUM(interaction_3month) AS customer_interactions_3month,
   SUM(interaction_6month) AS customer_interactions_6month,
   SUM(CASE WHEN int_type = 'Web' THEN interaction_1month ELSE 0 END) AS web_interactions_1month,
   SUM(CASE WHEN int_type = 'Web' THEN interaction_3month ELSE 0 END) AS web_interactions_3month,
   SUM(CASE WHEN int_type = 'Web' THEN interaction_6month ELSE 0 END) AS web_interactions_6month,
   SUM(CASE WHEN int_type = 'Text' THEN interaction_1month ELSE 0 END) AS text_interactions_1month,
   SUM(CASE WHEN int_type = 'Text' THEN interaction_3month ELSE 0 END) AS text_interactions_3month,
   SUM(CASE WHEN int_type = 'Text' THEN interaction_6month ELSE 0 END) AS text_interactions_6month,
   SUM(CASE WHEN int_type = 'Tele' THEN interaction_1month ELSE 0 END) AS tele_interactions_1month,
   SUM(CASE WHEN int_type = 'Tele' THEN interaction_3month ELSE 0 END) AS tele_interactions_3month,
   SUM(CASE WHEN int_type = 'Tele' THEN interaction_6month ELSE 0 END) AS tele_interactions_6month,
   SUM(CASE WHEN int_type = 'F2F' THEN interaction_1month ELSE 0 END) AS f2f_interactions_1month,
   SUM(CASE WHEN int_type = 'F2F' THEN interaction_3month ELSE 0 END) AS f2f_interactions_3month,
   SUM(CASE WHEN int_type = 'F2F' THEN interaction_6month ELSE 0 END) AS f2f_interactions_6month,
   MIN(as_at_date - customer_contact_start_date) AS last_interaction_days,
   MIN(CASE WHEN int_type = 'Web' THEN as_at_date - customer_contact_start_date END) AS web_last_interaction_days,
   MIN(CASE WHEN int_type = 'Tele' THEN as_at_date - customer_contact_start_date END) AS tele_last_interaction_days,
   MIN(CASE WHEN int_type NOT IN ('Web', 'Tele') THEN as_at_date - customer_contact_start_date END) AS other_last_interaction_days
  FROM interactions
  GROUP BY policy_number,
           as_at_date,
           svx_customer_key) a
  LEFT JOIN (
  SELECT policy_number, 
         as_at_date, 
         svx_customer_key,
         MAX(CASE WHEN int_type = 'Web' THEN latest_duration END) AS web_latest_duration,
         MAX(CASE WHEN int_type = 'Tele' THEN latest_duration END) AS tele_latest_duration
  FROM (
      SELECT DISTINCT policy_number, 
                      as_at_date, 
                      svx_customer_key,
                      int_type, 
                      FIRST_VALUE(duration) OVER (PARTITION BY policy_number, as_at_date, int_type ORDER BY customer_contact_start_date DESC) AS latest_duration
      FROM interactions
      WHERE duration > INTERVAL '0 days') foo 
      GROUP BY policy_number,
               as_at_date,
               svx_customer_key) b
  ON a.policy_number = b.policy_number 
  AND a.as_at_date = b.as_at_date
  AND a.svx_customer_key = b.svx_customer_key
),

-- Take summaries (max and min) of historical interaction activity involved with customer at as_at_date of interest
-- One row per customer
interactions_summary_cust AS (
  SELECT 
       policy_number,
       as_at_date,
       MAX(customer_interactions_1month) AS max_customer_interactions_1month,
       MIN(customer_interactions_1month) AS min_customer_interactions_1month,
       MAX(customer_interactions_3month) AS max_customer_interactions_3month,
       MIN(customer_interactions_3month) AS min_customer_interactions_3month,
       MAX(customer_interactions_6month) AS max_customer_interactions_6month,
       MIN(customer_interactions_6month) AS min_customer_interactions_6month,
       MAX(web_interactions_1month) AS max_web_interactions_1month,
       MIN(web_interactions_1month) AS min_web_interactions_1month,
       MAX(web_interactions_3month) AS max_web_interactions_3month,
       MIN(web_interactions_3month) AS min_web_interactions_3month,
       MAX(web_interactions_6month) AS max_web_interactions_6month,
       MIN(web_interactions_6month) AS min_web_interactions_6month,
       MAX(tele_interactions_1month) AS max_tele_interactions_1month,
       MIN(tele_interactions_1month) AS min_tele_interactions_1month,
       MAX(tele_interactions_3month) AS max_tele_interactions_3month,
       MIN(tele_interactions_3month) AS min_tele_interactions_3month,
       MAX(tele_interactions_6month) AS max_tele_interactions_6month,
       MIN(tele_interactions_6month) AS min_tele_interactions_6month,
       MAX(text_interactions_1month) AS max_text_interactions_1month,
       MIN(text_interactions_1month) AS min_text_interactions_1month,
       MAX(text_interactions_3month) AS max_text_interactions_3month,
       MIN(text_interactions_3month) AS min_text_interactions_3month,
       MAX(text_interactions_6month) AS max_text_interactions_6month,
       MIN(text_interactions_6month) AS min_text_interactions_6month,
       MAX(f2f_interactions_1month) AS max_f2f_interactions_1month,
       MIN(f2f_interactions_1month) AS min_f2f_interactions_1month,
       MAX(f2f_interactions_3month) AS max_f2f_interactions_3month,
       MIN(f2f_interactions_3month) AS min_f2f_interactions_3month,
       MAX(f2f_interactions_6month) AS max_f2f_interactions_6month,
       MIN(f2f_interactions_6month) AS min_f2f_interactions_6month,
       MIN(last_interaction_days) AS min_last_interaction_days,
       MAX(last_interaction_days) AS max_last_interaction_days,
       MIN(web_last_interaction_days) AS min_web_last_interaction_days,
       MAX(web_last_interaction_days) AS max_web_last_interaction_days,
       MIN(tele_last_interaction_days) AS min_tele_last_interaction_days,
       MAX(tele_last_interaction_days) AS max_tele_last_interaction_days,
       MIN(other_last_interaction_days) AS min_other_last_interaction_days,
       MAX(other_last_interaction_days) AS max_other_last_interaction_days,
       MIN(web_latest_duration) AS min_web_latest_duration,
       MAX(web_latest_duration) AS max_web_latest_duration,
       MIN(tele_latest_duration) AS min_tele_latest_duration,
       MAX(tele_latest_duration) AS max_tele_latest_duration
  FROM interactions_summary
  GROUP BY policy_number,
           as_at_date
),

ctp_motor_match_summary AS(
SELECT 
	a.policy_number,
	a.as_at_date,
  a.svxr_customer_key,
	SUM(CASE WHEN b.claim_number is null THEN 1 ELSE 0 end) as cpt_count_not_match_motor,
	SUM(CASE WHEN b.claim_number is null 
		AND(a.hist_lodgement_date BETWEEN date(a.as_at_date - INTERVAL '1 year')AND a.as_at_date) 
		then 1 ELSE 0 end) as cpt_count_not_match_motor_1yr,
	SUM(CASE WHEN b.claim_number is null 
		AND(a.hist_lodgement_date BETWEEN date(a.as_at_date - INTERVAL '2 year')AND a.as_at_date) 
		then 1 ELSE 0 end) as cpt_count_not_match_motor_2yr,
	SUM(CASE WHEN b.claim_number is null 
		AND(a.hist_lodgement_date BETWEEN date(a.as_at_date - INTERVAL '5 year')AND a.as_at_date) 
		then 1 ELSE 0 end) as cpt_count_not_match_motor_5yr	
FROM claim_key_table a
LEFT JOIN customer_key_table_motor b
on a.svxr_customer_key = b.svxr_customer_key
   and a.hist_claim_loss_date=b.claim_loss_date
WHERE a.hist_loss_type ='CTP'
group by a.policy_number,
		     a.as_at_date,
	       a.svxr_customer_key
),

ctp_motor_match_summary_cust AS(
SELECT 
		policy_number,
		as_at_date,
		MAX(cpt_count_not_match_motor) as max_cpt_count_not_match_motor,
		MAX(cpt_count_not_match_motor_1yr) as max_cpt_count_not_match_motor_1yr,
		MAX(cpt_count_not_match_motor_2yr) as max_cpt_count_not_match_motor_2yr,
		MAX(cpt_count_not_match_motor_5yr) as max_cpt_count_not_match_motor_5yr
FROM ctp_motor_match_summary
group by policy_number,
		 as_at_date
),

claim_contact_merge AS(
SELECT  DISTINCT 
		    b.claim_number, 
        b.policy_number, 
        b.claim_lodgement_date AS as_at_date, 
        b.claim_loss_type_name,
        a.contact_full_name, 
        a.role_name, 
        a.contact_mobile_number,  
        a.address_line_1,
        a.email_address_1,
        row_number() over (partition by b.claim_number, a.contact_full_name, a.contact_mobile_number order by a.role_name) as id_row_num
FROM ctx.mv_cc_ci_claim_contact_ext a
INNER JOIN pub_core.mv_claim_header b
ON a.claim_number = b.claim_number 
WHERE    
        b.policy_issue_state='NSW' 
        AND b.notify_only_claim_flag = 'No'
),

hist_injured_party_summary AS(
SELECT 
       a.policy_number,
       a.as_at_date,
       a.contact_full_name,
       a.contact_mobile_number,
       COUNT (DISTINCT b.claim_number) AS IP_hist_claims_total_count,
       SUM(CASE WHEN b.role_name = 'Injured Party' THEN 1 ELSE 0 END) AS hist_IP_count_total,
       SUM(CASE WHEN b.role_name= 'Insured' THEN 1 ELSE 0 END) AS hist_IP_insured_count_total,
       SUM(CASE WHEN b.role_name= 'Driver' THEN 1 ELSE 0 END) AS hist_IP_driver_count_total,
       SUM(CASE WHEN b.role_name= 'Cheque Payee' THEN 1 ELSE 0 END) AS hist_IP_payee_count_total,
       SUM(CASE WHEN b.id_row_num=1 AND b.claim_loss_type_name = 'CTP' THEN 1 ELSE 0 END) AS hist_IP_ctp_count_total,
       SUM(CASE WHEN b.id_row_num=1 AND b.claim_loss_type_name = 'Motor' THEN 1 ELSE 0 END) AS hist_IP_motor_count_total
FROM claim_contact_merge a
INNER JOIN claim_contact_merge b
ON a.contact_full_name = b.contact_full_name 
   AND (a.contact_mobile_number = b.contact_mobile_number
   OR a.address_line_1 = b.address_line_1
   OR a.email_address_1 = b.email_address_1)
   AND b.as_at_date< a.as_at_date
WHERE   a.claim_loss_type_name = 'CTP' 
        AND a.role_name='Injured Party'
GROUP BY 
	       a.policy_number,
	       a.as_at_date,
	       a.contact_full_name,
	       a.contact_mobile_number
	       
),

injured_party_summary_cust AS(
SELECT 
       policy_number,
       as_at_date,
       MAX (IP_hist_claims_total_count) AS max_IP_hist_claims_total_count,
       MAX(hist_IP_count_total) AS max_hist_IP_count_total,
       MAX(hist_IP_insured_count_total) AS max_hist_IP_insured_count_total,
       MAX(hist_IP_driver_count_total) AS max_hist_IP_driver_count_total,
       MAX(hist_IP_payee_count_total) AS max_hist_IP_payee_count_total,
       MAX(hist_IP_ctp_count_total) AS max_hist_IP_ctp_count_total,
       MAX(hist_IP_motor_count_total) AS max_hist_IP_motor_count_total
FROM hist_injured_party_summary a
GROUP BY 
	       policy_number,
	       as_at_date
)


SELECT
      a.*,
	    COALESCE(b.max_num_complaints_3yr,0) AS cust_max_num_complaints_3yr,
	    COALESCE(b.max_sales_complaints_3yr,0) AS cust_max_sales_complaints_3yr,
	    COALESCE(b.max_claims_complaints_3yr,0) AS cust_max_claims_complaints_3yr,
	    COALESCE(b.max_iag_favour_complaints_3yr,0) AS cust_max_iag_favour_complaints_3yr,
	    COALESCE(b.max_customer_favour_complaints_3yr,0) AS cust_max_customer_favour_complaints_3yr,
	    COALESCE(b.max_outstanding_complaints_3yr,0) AS cust_max_outstanding_complaints_3yr,
	    COALESCE(b.max_level_0_complaints_3yr,0) AS cust_max_level_0_complaints_3yr,
	    COALESCE(b.max_level_1_complaints_3yr,0) AS cust_max_level_1_complaints_3yr,
	    COALESCE(b.max_level_2_complaints_3yr,0) AS cust_max_level_2_complaints_3yr,
	    COALESCE(b.max_level_3_complaints_3yr,0) AS cust_max_level_3_complaints_3yr,
	    COALESCE(b.max_claim_rejection_complaints_3yr,0) AS cust_max_claim_rejection_complaints_3yr,
	    COALESCE(b.min_last_complaint_days,0) AS cust_min_last_complaint_days,
	    COALESCE(c.max_policy_count,0) AS cust_max_policy_count,
	    COALESCE(c.max_active_policies_count,0) AS cust_max_active_policies_count,
	    COALESCE(c.max_cancel_policies_count,0) AS cust_max_cancel_policies_count,
	    COALESCE(c.max_lapse_policies_count,0) AS cust_max_lapse_policies_count,
	    COALESCE(d.max_investigation_count,0) AS cust_max_investigation_count,
	    COALESCE(d.max_denial_count,0) AS cust_max_denial_count,
	    COALESCE(d.max_manual_referral_count,0) AS cust_max_manual_referral_count,
	    COALESCE(d.max_system_alert_count,0) AS cust_max_system_alert_count,
	    COALESCE(d.max_under_investigation_count,0) AS max_under_investigation_count,
	    d.min_last_investigation_days AS cust_min_last_investigation_days,
	    d.min_last_manual_referral_days AS cust_min_last_manual_referral_days,   
      COALESCE(e.sum_claim_count_total,0) AS cust_sum_claim_count_total,
      COALESCE(e.sum_ctp_claim_count_total,0) AS cust_sum_ctp_claim_count_total,
      COALESCE(e.max_claim_count_total,0) AS cust_max_claim_count_total,
      COALESCE(e.max_ctp_claim_count_total,0) AS cust_max_ctp_claim_count_total,
      COALESCE(e.max_insured_claim_count_total,0) AS cust_max_insured_claim_count_total,
      COALESCE(e.max_claim_count_3_month,0) AS cust_max_claim_count_3_month,
      COALESCE(e.max_claim_count_6_month,0) AS cust_max_claim_count_6_month,
      COALESCE(e.max_claim_count_1yr,0) AS cust_max_claim_count_1yr,
      COALESCE(e.max_claim_count_2yr,0) AS cust_max_claim_count_2yr,
      COALESCE(e.max_claim_count_5yr,0) AS cust_max_claim_count_5yr,
      COALESCE(e.max_claim_count_3_month_motor,0) AS cust_max_claim_count_3_month_motor,
      COALESCE(e.max_claim_count_3_month_ctp,0) AS cust_max_claim_count_3_month_ctp,
      COALESCE(e.max_claim_count_3_month_collision,0) AS cust_max_claim_count_3_month_collision,
      COALESCE(e.max_claim_count_3_month_insured_fault_ctp, 0) AS cust_max_claim_count_3_month_insured_fault_ctp,
      COALESCE(e.max_claim_count_3_month_unknown_fault_ctp, 0) AS cust_max_claim_count_3_month_unknown_fault_ctp,
      COALESCE(e.max_claim_count_3_month_insured_third_party_ctp, 0) AS cust_max_claim_count_3_month_insured_third_party_ctp,
      COALESCE(e.max_claim_count_3_month_pedestrian_ctp, 0) AS cust_max_claim_count_3_month_pedestrian_ctp,
      COALESCE(e.max_claim_count_3_month_multi_ctp, 0) AS cust_max_claim_count_3_month_multi_ctp,
      COALESCE(e.max_claim_count_3_month_cause_unknown_ctp, 0) AS cust_max_claim_count_3_month_cause_unknown_ctp,
      COALESCE(e.max_claim_count_3_month_insured_fault, 0) AS cust_max_claim_count_3_month_insured_fault,
      COALESCE(e.max_claim_count_3_month_insured_third_party,0) AS cust_max_claim_count_3_month_insured_third_party,
      COALESCE(e.max_claim_count_3_month_sva, 0) AS cust_max_claim_count_3_month_sva,
      COALESCE(e.max_claim_count_6_month_motor, 0) AS cust_max_claim_count_6_month_motor,
      COALESCE(e.max_claim_count_6_month_ctp, 0) AS cust_max_claim_count_6_month_ctp,
      COALESCE(e.max_claim_count_6_month_collision, 0) AS cust_max_claim_count_6_month_collision,
      COALESCE(e.max_claim_count_6_month_insured_fault_ctp, 0) AS cust_max_claim_count_6_month_insured_fault_ctp,
      COALESCE(e.max_claim_count_6_month_unknown_fault_ctp, 0) AS cust_max_claim_count_6_month_unknown_fault_ctp,
      COALESCE(e.max_claim_count_6_month_insured_third_party_ctp, 0) AS cust_max_claim_count_6_month_insured_third_party_ctp,
      COALESCE(e.max_claim_count_6_month_pedestrian_ctp, 0) AS cust_max_claim_count_6_month_pedestrian_ctp,
      COALESCE(e.max_claim_count_6_month_multi_ctp, 0) AS cust_max_claim_count_6_month_multi_ctp,
      COALESCE(e.max_claim_count_6_month_cause_unknown_ctp, 0) AS cust_max_claim_count_6_month_cause_unknown_ctp,
      COALESCE(e.max_claim_count_6_month_insured_fault, 0) AS cust_max_claim_count_6_month_insured_fault,
      COALESCE(e.max_claim_count_6_month_insured_third_party, 0) AS cust_max_claim_count_6_month_insured_third_party,
      COALESCE(e.max_claim_count_6_month_sva, 0) AS cust_max_claim_count_6_month_sva,
      COALESCE(e.max_claim_count_1yr_motor, 0) AS cust_max_claim_count_1yr_motor,
      COALESCE(e.max_claim_count_1yr_ctp, 0) AS cust_max_claim_count_1yr_ctp,
      COALESCE(e.max_claim_count_1yr_collision, 0) AS cust_max_claim_count_1yr_collision,
      COALESCE(e.max_claim_count_1yr_insured_fault_ctp, 0) AS cust_max_claim_count_1yr_insured_fault_ctp,
      COALESCE(e.max_claim_count_1yr_unknown_fault_ctp, 0) AS cust_max_claim_count_1yr_unknown_fault_ctp,
      COALESCE(e.max_claim_count_1yr_insured_third_party_ctp, 0) AS cust_max_claim_count_1yr_insured_third_party_ctp,
      COALESCE(e.max_claim_count_1yr_pedestrian_ctp, 0) AS cust_max_claim_count_1yr_pedestrian_ctp,
      COALESCE(e.max_claim_count_1yr_multi_ctp, 0) AS cust_max_claim_count_1yr_multi_ctp,
      COALESCE(e.max_claim_count_1yr_cause_unknown_ctp, 0) AS cust_max_claim_count_1yr_cause_unknown_ctp,
      COALESCE(e.max_claim_count_1yr_insured_fault, 0) AS cust_max_claim_count_1yr_insured_fault,
      COALESCE(e.max_claim_count_1yr_insured_third_party, 0) AS cust_max_claim_count_1yr_insured_third_party,
      COALESCE(e.max_claim_count_1yr_sva, 0) AS cust_max_claim_count_1yr_sva,
      COALESCE(e.max_claim_count_2yr_motor, 0) AS cust_max_claim_count_2yr_motor,
      COALESCE(e.max_claim_count_2yr_ctp, 0) AS cust_max_claim_count_2yr_ctp,
      COALESCE(e.max_claim_count_2yr_collision, 0) AS cust_max_claim_count_2yr_collision,
      COALESCE(e.max_claim_count_2yr_insured_fault_ctp, 0) AS cust_max_claim_count_2yr_insured_fault_ctp,
      COALESCE(e.max_claim_count_2yr_unknown_fault_ctp, 0) AS cust_max_claim_count_2yr_unknown_fault_ctp,
      COALESCE(e.max_claim_count_2yr_insured_third_party_ctp, 0) AS cust_max_claim_count_2yr_insured_third_party_ctp,
      COALESCE(e.max_claim_count_2yr_pedestrian_ctp, 0) AS cust_max_claim_count_2yr_pedestrian_ctp,
      COALESCE(e.max_claim_count_2yr_multi_ctp, 0) AS cust_max_claim_count_2yr_multi_ctp,
      COALESCE(e.max_claim_count_2yr_cause_unknown_ctp, 0) AS cust_max_claim_count_2yr_cause_unknown_ctp,
      COALESCE(e.max_claim_count_2yr_insured_fault, 0) AS cust_max_claim_count_2yr_insured_fault,
      COALESCE(e.max_claim_count_2yr_insured_third_party, 0) AS cust_max_claim_count_2yr_insured_third_party,
      COALESCE(e.max_claim_count_2yr_sva, 0) AS cust_max_claim_count_2yr_sva,
      COALESCE(e.max_claim_count_5yr_motor, 0) AS cust_max_claim_count_5yr_motor,
      COALESCE(e.max_claim_count_5yr_ctp, 0) AS cust_max_claim_count_5yr_ctp,
      COALESCE(e.max_claim_count_5yr_collision, 0) AS cust_max_claim_count_5yr_collision,
      COALESCE(e.max_claim_count_5yr_insured_fault_ctp, 0) AS cust_max_claim_count_5yr_insured_fault_ctp,
      COALESCE(e.max_claim_count_5yr_unknown_fault_ctp, 0) AS cust_max_claim_count_5yr_unknown_fault_ctp,
      COALESCE(e.max_claim_count_5yr_insured_third_party_ctp, 0) AS cust_max_claim_count_5yr_insured_third_party_ctp,
      COALESCE(e.max_claim_count_5yr_pedestrian_ctp, 0) AS cust_max_claim_count_5yr_pedestrian_ctp,
      COALESCE(e.max_claim_count_5yr_multi_ctp, 0) AS cust_max_claim_count_5yr_multi_ctp,
      COALESCE(e.max_claim_count_5yr_cause_unknown_ctp, 0) AS cust_max_claim_count_5yr_cause_unknown_ctp,
      COALESCE(e.max_claim_count_5yr_insured_fault, 0) AS cust_max_claim_count_5yr_insured_fault,
      COALESCE(e.max_claim_count_5yr_insured_third_party, 0) AS cust_max_claim_count_5yr_insured_third_party, 
      COALESCE(e.max_claim_count_5yr_sva, 0) AS cust_max_claim_count_5yr_sva, 
	    COALESCE(f.max_number_cancel_arrear_3yr, 0) AS cust_max_number_cancel_arrears_3yr,
	    COALESCE(f.max_number_cancel_arrear_3yr_motor, 0) AS cust_max_number_cancel_arrears_3yr_motor,
	    COALESCE(f.max_number_cancel_arrear_3yr_home, 0) AS cust_max_number_cancel_arrears_3yr_home,
	    COALESCE(f.max_number_cancel_arrear_3yr_ctp, 0) AS cust_max_number_cancel_arrears_3yr_ctp,
	    COALESCE(f.min_last_cancel_arrear_days, 0) AS cust_min_last_cancel_arrear_days,
	    g.max_customer_interactions_1month AS cust_max_customer_interactions_1month,
	    g.max_customer_interactions_3month AS cust_max_customer_interactions_3month,
	    g.max_customer_interactions_6month AS cust_max_customer_interactions_6month,
	    g.max_web_interactions_1month AS cust_max_web_interactions_1month,
	    g.max_web_interactions_3month AS cust_max_web_interactions_3month,
	    g.max_web_interactions_6month AS cust_max_web_interactions_6month,
	    g.max_text_interactions_1month AS cust_max_text_interactions_1month,
	    g.max_text_interactions_3month AS cust_max_text_interactions_3month,
	    g.max_text_interactions_6month AS cust_max_text_interactions_6month,
	    g.max_tele_interactions_1month AS cust_max_tele_interactions_1month,
	    g.max_tele_interactions_3month AS cust_max_tele_interactions_3month,
	    g.max_tele_interactions_6month AS cust_max_tele_interactions_6month,
	    g.max_f2f_interactions_1month AS cust_max_f2f_interactions_1month,
	    g.max_f2f_interactions_3month AS cust_max_f2f_interactions_3month,
	    g.max_f2f_interactions_6month AS cust_max_f2f_interactions_6month,
	    g.max_last_interaction_days AS cust_max_last_interaction_days,
	    g.max_web_last_interaction_days AS cust_max_web_last_interaction_days,
	    g.max_tele_last_interaction_days AS cust_max_tele_last_interaction_days,
	    g.max_other_last_interaction_days AS cust_max_other_last_interaction_days,
	    g.max_web_latest_duration AS cust_max_web_latest_duration,
	    g.max_tele_latest_duration AS cust_max_tele_latest_duration,
	    g.min_customer_interactions_1month AS cust_min_customer_interactions_1month,
	    g.min_customer_interactions_3month AS cust_min_customer_interactions_3month,
	    g.min_customer_interactions_6month AS cust_min_customer_interactions_6month,
	    g.min_web_interactions_1month AS cust_min_web_interactions_1month,
	    g.min_web_interactions_3month AS cust_min_web_interactions_3month,
	    g.min_web_interactions_6month AS cust_min_web_interactions_6month,
	    g.min_text_interactions_1month AS cust_min_text_interactions_1month,
	    g.min_text_interactions_3month AS cust_min_text_interactions_3month,
	    g.min_text_interactions_6month AS cust_min_text_interactions_6month,
	    g.min_tele_interactions_1month AS cust_min_tele_interactions_1month,
	    g.min_tele_interactions_3month AS cust_min_tele_interactions_3month,
	    g.min_tele_interactions_6month AS cust_min_tele_interactions_6month,
	    g.min_f2f_interactions_1month AS cust_min_f2f_interactions_1month,
	    g.min_f2f_interactions_3month AS cust_min_f2f_interactions_3month,
	    g.min_f2f_interactions_6month AS cust_min_f2f_interactions_6month,
	    g.min_last_interaction_days AS cust_min_last_interaction_days,
	    g.min_web_last_interaction_days AS cust_min_web_last_interaction_days,
	    g.min_tele_last_interaction_days AS cust_min_tele_last_interaction_days,
	    g.min_other_last_interaction_days AS cust_min_other_last_interaction_days,
	    g.min_web_latest_duration AS cust_min_web_latest_duration,
	    g.min_tele_latest_duration AS cust_min_tele_latest_duration,
	    COALESCE(h.max_cpt_count_not_match_motor,0) AS max_cpt_count_not_match_motor,
	    COALESCE(h.max_cpt_count_not_match_motor_1yr,0) AS max_cpt_count_not_match_motor_1yr,
	    COALESCE(h.max_cpt_count_not_match_motor_2yr,0) AS max_cpt_count_not_match_motor_2yr,
	    COALESCE(h.max_cpt_count_not_match_motor_5yr,0) AS max_cpt_count_not_match_motor_5yr,
      COALESCE(k.max_IP_hist_claims_total_count, 0) AS max_IP_hist_claims_total_count,
      COALESCE(k.max_hist_IP_count_total, 0) AS max_hist_IP_count_total,
      COALESCE(k.max_hist_IP_insured_count_total, 0) AS max_hist_IP_insured_count_total,
      COALESCE(k.max_hist_IP_driver_count_total, 0) AS max_hist_IP_driver_count_total,
      COALESCE(k.max_hist_IP_payee_count_total, 0) AS max_hist_IP_payee_count_total,
      COALESCE(k.max_hist_IP_ctp_count_total, 0) AS max_hist_IP_ctp_count_total,
      COALESCE(k.max_hist_IP_motor_count_total, 0) AS max_hist_IP_motor_count_total 
FROM customer_summary a
LEFT JOIN complaint_summary_cust b
  ON a.policy_number = b.policy_number
  AND a.as_at_date = b.as_at_date
LEFT JOIN policy_summary_cust c 
  ON a.policy_number = c.policy_number
  AND a.as_at_date = c.as_at_date 
LEFT JOIN investigation_summary_cust d 
  ON a.policy_number = d.policy_number
  AND a.as_at_date = d.as_at_date
LEFT JOIN claims_summary_cust e 
  ON a.policy_number = e.policy_number
  AND a.as_at_date = e.as_at_date 
LEFT JOIN policy_arrears_summary_cust f
  ON a.policy_number = f.policy_number
  AND a.as_at_date = f.as_at_date
LEFT JOIN interactions_summary_cust g
  ON a.policy_number = g.policy_number
  AND a.as_at_date = g.as_at_date
LEFT JOIN ctp_motor_match_summary_cust h
  ON a.policy_number = h.policy_number
  AND a.as_at_date = h.as_at_date  
LEFT JOIN injured_party_summary_cust k
  ON a.policy_number = k.policy_number
  AND a.as_at_date = k.as_at_date  
        
