-- -------------------------------------------------------------------------------------------------
-- Author:      Ana Namvar 
-- DescriptiON: risk area at Claims Lodgement &  Liability (CTP_indicators_claim_level)                                                                                
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- VERSIONS   DATE        WHO         			  DESCRIPTION
-- 1.0        19/01/2024  Ana Namvar  	Initial versiON
-- 2.0        06/03/2024  Ana Namvar  	V2
-- 3.0        17/03/2024  Ana Namvar    v3
-- 4.0        26/03/2024  Ana Namvar    v4 
-- --------------------------------------------------------------------------------------------



{{ config(enabled=false) }}

--risk area at Claims Lodgement &  Liability (CTP_indicators)
-- claim level

with claim AS (
	SELECT 
		* 
	FROM ctx.mv_cc_ci_claim_header_ext mccche 
	WHERE claim_status_name <> 'Draft' 
	AND ctp_statutory_insurer_state_name IN ('NSW')
	AND line_of_business_name = 'Compulsory Third Party'
	AND notify_ONly_claim_flag = 'No' 
	AND policy_number <> 'NDS*11111111'),
	
-- single vehicle accident 
ctp_vehicle AS (
	SELECT 
		claim_number as vehicle_claim_number,
   		policy_id,
   		count(DISTINCT vehicle_id) count_vehicle_involve
	FROM ctx.mv_cc_ci_incident_driver_vehicle_ext mccidve 
	GROUP BY 1,2 ), 
	
	--Late_claim
	--claim_late_name : Flag to indicate whether the claim is late or not. Takes Value Yes/No
ctp_late_claim AS (
	SELECT 
		cs.claim_number,
		Max(case when cs.claim_late_name = 'Yes' then 1 else 0 end ) late_claim_flag
	FROM pub.mv_ctp_claim_summary cs
	group by claim_number
),
  
  
  --delay in obtaining medical treatment > 5 days 
injury as 
(select
 	claim_number as injury_claim_number
 	,min(first_medical_exam_date) first_medical_exam_date
 from 
 	pub.ctp_injury 
 group by 1
),
  	
late_treatment as 
(select
	min (recovery_plan_start_date) recovery_plan_start_date
	,claim_number as treatment_claim_number
from 
	pub.mv_ctp_claim_summary 
group by 2), 

first_treatment as (
select
	claim.claim_number
	,min(medical_treatment_start_date) treatment_start_date 
from 
	ctx.cc_ci_medicaltreatment join claim on claim.claim_id = cc_ci_medicaltreatment.claim_id
group by 1),

-- delay in obtaining medical treatment > 5 days  using fitness certificate 
first_fitness_certificate as (
select 
	claim_number as fitness_claim_number
	--,claim_exposure_id
	--,fitness_certificate_id
	,min (distinct cast(fitness_certificate_issue_date as date)) as       min_fitness_certificate_issue_date
	,count(distinct fitness_certificate_id) as total_certificates
from 
	pub_core.claim_exposure_weekly_benefit_summary
group by 1
)
--treatment_min_issue as (
--select 
--	distinct ffc.claim_number
--	,ffc.min_fitness_certificate_issue_date
--	,cewbs.fitness_certificate_type_code
--	,cewbs.fitness_certificate_type_name
--	,cewbs.pre_accident_employment_status_name
--	,cewbs.pre_accident_date_ceased_work
--	,cewbs.fitness_certificate_start_date
--from first_fitness_certificate ffc
--join pub_core.claim_exposure_weekly_benefit_summary cewbs
--	on ffc.claim_number = cewbs.claim_number
--	and ffc.min_fitness_certificate_issue_date = cast(cewbs.fitness_certificate_issue_date as date))
,	

-- witness
Witness as (
	select
	dccccr.contact_role_create_date
	,dccccr.contact_role_updated_date
	,dccccr.role_name
	,dccccr.role_id
	,dccccr.role_code
	,dccccr.contact_role_contact_id
	,dccccr.claim_contact_id
	,dccccr.independent_witness_flag_name
	,dccccr.independent_witness_flag_id
	,dccccr.exposure_id
	,dccccr.incident_id
	,dcccc.claim_id 
	from ctx.ds_cc_ci_claims_contact_role dccccr 
	inner join ctx.ds_cc_ci_claims_contact dcccc
		on dcccc.claim_contact_id  = dccccr.claim_contact_id 
		where dccccr.role_name = 'Witness'
	 )

,
	
independent_witness as(
	select 
		claim.claim_number, 
		count(distinct Witness.contact_role_contact_id) total_number_of_witness,
		count(distinct case when Witness.contact_role_updated_date > claim.claim_lodgement_date then Witness.contact_role_contact_id else null end) total_witness_after_lodgment,
		count(distinct case when Witness.independent_witness_flag_id = '1' then Witness.contact_role_contact_id else null end) total_independent_witness,
		count(distinct case when Witness.independent_witness_flag_id = '1' and Witness.contact_role_updated_date > claim.claim_lodgement_date 
		then Witness.contact_role_contact_id else null end) total_independent_witness_after_lodgment
	from claim 
	left join Witness 
		on claim.claim_id = Witness.claim_id
	group by 1)
,

--------------- witness has relationship with IP 	
	 witness_has_family_relationship as (
select 
	distinct t1.claim_number
	,count(t1.contact_id ) total_witness_has_family_relationship
from ctx.mv_cc_ci_claim_contact_ext t1
	where t1.role_name ='Witness'
	and exists (
		select 
		t2.claim_number 
		,t2.contact_id  
	from ctx.mv_cc_ci_claim_contact_ext t2
		where  
		(t2.role_name = 'Partner/Spouse' or t2.role_name= 'Insured\'s Spouse'or t2.role_name ='Insured''s Relative' or t2.role_name='Parent/Guardian' or t2.role_name='Family Member' or t2.role_name='Insured''s Relative' )  
		and t1.claim_number  = t2.claim_number
		and t1.contact_id = t2.contact_id
	)
	group by 1)
			

	, 

-- complaint
-- immadiate complaint with minor injury 
--  delay complaint after accident 
		
minor_injury as( 
select
	claim.claim_id
from claim
inner join ctx.mv_cc_ci_incident mcci
	on claim.claim_id = mcci.claim_id 
where incident_severity_name like ('%Minor%'))
,
	
complaint as (
select 
	claim.claim_number
	,count(distinct Minor_injury.claim_id) minor_injury_flag
	,count(complains.complaint_claim_number) number_of_complains
	,min (complaint_received_date - claim.claim_loss_date) first_complaint
from claim 
left join pub_restricted.mv_svx_iag_customer_activity_complaints complains 
	on complains.complaint_claim_number = claim.claim_number 
left join Minor_injury 
	on Minor_injury.claim_id = claim.claim_id
group by 1 )

,

delay_complaint_after_claim as (
select 
	claim.claim_number
	,min(complaint_received_date) first_complaint
	,min(complaint_received_date - claim.claim_loss_date) delay_complaint_received
from claim 
left join pub_restricted.mv_svx_iag_customer_activity_complaints complains 
	on complains.complaint_claim_number = claim.claim_number 
group by 1 )

,

hist_cust_policy AS (
SELECT
	svx_customer_key,
	REGEXP_REPLACE(svx_policy_key, '^[A-Z]+-', '', 'g') AS svx_policy_key,
	policy_type_source,
	MIN(customer_policy_start_date) AS customer_policy_start_date,
	MAX(customer_policy_end_date) AS customer_policy_end_date,
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
	AND customer_policy_start_date <= CURRENT_DATE 
	and policy_type_source = 'CTP'
	and customer_policy_role in ('POLICY HOLDER','DRIVER')
GROUP BY
svx_customer_key,
svx_policy_key,
policy_type_source)
,

-- Relevant customer details where the customer was on the policy before as at date
-- Includes one row per customer related to claim in question
customer_key_table AS ( 
SELECT 
  ch.policy_number, 
  ch.claim_lodgement_date AS as_at_date,    
  cp.svx_customer_key,
  ch.claim_number
FROM  
  claim ch 
INNER JOIN 
  hist_cust_policy cp    
ON  ch.policy_number = cp.svx_policy_key    
AND cp.customer_policy_start_date <= ch.claim_lodgement_date    
  GROUP BY  
  ch.policy_number,   
  ch.claim_lodgement_date,
  cp.svx_customer_key,
  ch.claim_number
)
, 

-- total number of submitted claim in the same policy before current claim any time before
cte_cnt_claim_plcy as (
select
	t1.claim_number
	,count(distinct t2.claim_number) total_past_claims_same_policy
	,min(t2.as_at_date) first_submitted_claim
	,max(t2.as_at_date) last_submitted_claim 
from 
	customer_key_table t1
left join 
	customer_key_table t2
on t1.policy_number = t2.policy_number
and t1.as_at_date > t2.as_at_date 
	group by 1)
, 

-- total number of submitted claims but the same customer before current claim in last 90 days
cte_cnt_claim_90 as (
select
	t1.claim_number
	,count(distinct t2.claim_number) total_past_claims_90
from 
	customer_key_table t1 
left join 
	customer_key_table t2 
on t1.svx_customer_key = t2.svx_customer_key 
and t1.as_at_date - t2.as_at_date > 0 
and  t1.as_at_date - t2.as_at_date <= 90
	group by 1 
	order by 2 desc )

-- count of claims before current CTP claim (all claims including CTP , motor , ...) 
-- ?  using main claim table  ctx.mv_cc_ci_claim_header_ext  


,
--indicator 10  	
 customer_claim_exposure  as (
select
	distinct cc.claim_number
	,cc.claim_id 
	,cc.contact_id,
	CASE WHEN role_name='Injured Party' THEN 1 ELSE 0 END AS injured_flag
	,eh.policy_number
	,cus.svx_customer_key
	,claim_lodgement_date
	,claim_loss_date
	,claim_Exposure_type_code
	,CASE WHEN claim_Exposure_type_code='CTP' THEN 1 ELSE 0 END AS CTP_claim_flag 
	,CASE WHEN claim_Exposure_type_code='property damage' THEN 1 ELSE 0 END AS propoerty_damage_flag 
from 
	ctx.mv_cc_ci_claim_contact_ext cc
join 
	ctx.mv_cc_ci_claim_exposure_header_ext eh
on 
	cc.claim_number = eh.claim_number 
join 
	pub_restricted.mv_svx_iag_customer_policy  cus
on 
	REGEXP_REPLACE(cus.svx_policy_key, '^[A-Z]+-', '', 'g') = eh.policy_number 
)  
, 

--ctp_ind_10 = injured person has had a previous CTP claim or 2 or more property damage claims in preceding 12 months
ctp_ind_10 as (
select 
	t1.claim_number 
	,count(distinct t2. claim_number) injured_total_past_claims
	,count(distinct case when t2.CTP_claim_flag ='1' then t2.claim_number else null end) injured_total_past_ctp_claims
	,count(distinct case when t2.propoerty_damage_flag ='1' then t2.claim_number else null end) injured_total_past_property_damage_claims 
from 
	customer_claim_exposure t1
left join 
	customer_claim_exposure t2
on 
	t1.svx_customer_key = t2.svx_customer_key 
where t1.claim_lodgement_date - t2.claim_lodgement_date > 0 
and   t1.claim_lodgement_date - t2.claim_lodgement_date <=365
and   t1.claim_lodgement_date > '2023-01-01'
	group by 1)

	select 
	claim.*,
	ctp_vehicle.count_vehicle_involve,
	CASE WHEN count_vehicle_involve <=1  THEN 1 ELSE 0 END AS single_vehicle_accident,
	claim.claim_lodgement_date - claim.claim_loss_date AS late_claim,
	ctp_late_claim.late_claim_flag,
	CASE WHEN late_claim_flag =1  THEN 'Yes' ELSE 'No' END AS late_claim_name,
	CAST(extract('hour' FROM claim.claim_loss_time) AS int) AS claim_loss_hour_num,
  	CASE WHEN extract('hour' FROM claim.claim_loss_time)  >= 21 or extract('hour' FROM claim_loss_time) <= 6 THEN 1 ELSE 0 END AS time_21_to_6,
  	injury.first_medical_exam_date,
  	late_treatment.recovery_plan_start_date,
  	cast(first_treatment.treatment_start_date as date) treatment_start_date,
  	cast(first_treatment.treatment_start_date as date) - cast(claim.claim_loss_date as date) delay_medical_treatment_medical,
  	first_fitness_certificate.min_fitness_certificate_issue_date,
  	first_fitness_certificate.total_certificates,
  	first_fitness_certificate.min_fitness_certificate_issue_date - cast(claim.claim_loss_date as date) as delay_certificate,
  	independent_witness.total_number_of_witness,
  	independent_witness.total_witness_after_lodgment,
  	independent_witness.total_independent_witness,
  	independent_witness.total_independent_witness_after_lodgment,
  	witness_has_family_relationship.total_witness_has_family_relationship,
  	complaint.minor_injury_flag,
  	complaint.number_of_complains,
  	complaint.first_complaint,
  	delay_complaint_after_claim.delay_complaint_received,#late_complaint_received_days
  	cte_cnt_claim_plcy.total_past_claims_same_policy,
  	cte_cnt_claim_plcy.first_submitted_claim,
  	cte_cnt_claim_plcy.last_submitted_claim,
  	cte_cnt_claim_90.total_past_claims_90, #cust_max_claim_count_3_month
  	ctp_ind_10.injured_total_past_claims, #max_hist_IP_count_total
  	ctp_ind_10.injured_total_past_ctp_claims, #max_hist_IP_ctp_count_total
  	ctp_ind_10.injured_total_past_property_damage_claims max_hist_IP_motor_count_total
  	
  	
	from claim 
	left join ctp_vehicle on ctp_vehicle.vehicle_claim_number = claim.claim_number and ctp_vehicle.policy_id = claim.policy_id
	left join ctp_late_claim on ctp_late_claim.claim_number = claim.claim_number
	left join injury on injury.injury_claim_number = claim.claim_number
	left join late_treatment on late_treatment.treatment_claim_number = claim.claim_number
	left join first_treatment on first_treatment.claim_number = claim.claim_number
	left join first_fitness_certificate on first_fitness_certificate.fitness_claim_number = claim.claim_number
	left join independent_witness on independent_witness.claim_number = claim.claim_number
	left join witness_has_family_relationship on witness_has_family_relationship.claim_number = claim.claim_number
	left join complaint on complaint.claim_number = claim.claim_number
	left join delay_complaint_after_claim on delay_complaint_after_claim.claim_number = claim.claim_number
	left join cte_cnt_claim_plcy on cte_cnt_claim_plcy.claim_number = claim.claim_number
	left join cte_cnt_claim_90 on cte_cnt_claim_90.claim_number = claim.claim_number
	left join ctp_ind_10 on ctp_ind_10.claim_number = claim.claim_number
	


	


  