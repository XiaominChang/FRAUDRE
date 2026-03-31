--Testing Scripts for MAD Modelling Dataset

-- 0-3 days claims removed
select 
	MIN(date_part('day',claim_loss_date - original_policy_inception_date_policy_data)),
	MAX(date_part('day',claim_loss_date - original_policy_inception_date_policy_data))
from
dl_analytics.mad_modelling_data_final_dbt
limit 10;

-- New Columns created - claim_loss_day_of_week, claim_loss_month,claim_loss_hour, claim_loss_year
select 
	claim_loss_date,
	claim_loss_day_of_week,
	claim_loss_month,
	claim_loss_time_at_lodgement,
	claim_loss_hour,
	claim_loss_year
from
dl_analytics.mad_modelling_data_final_dbt
limit 10;

-- Group cause_of_loss_at_lodgement
select distinct 
	general_nature_of_loss_at_lodgement,
	cause_of_loss_at_lodgement,
	collision_type_at_lodgement
from 
	dl_analytics.mad_modelling_data_final_dbt;


-- Check claim_how_reported_name
select distinct
	claim_how_reported_name_org,
	claim_how_reported_name
from 
	dl_analytics.mad_modelling_data_final_dbt;


-- check days_from_lodge_to_tp_vehicle_create
select distinct 
	tp_vehicle_create_time,
	claim_lodgement_date,
	days_from_lodge_to_tp_vehicle_create
from 
dl_analytics.mad_modelling_data_final_dbt
limit 10;

-- Check triage, investigation, and Denied withdrawn flag
select distinct
	triage_flag,
	investigation_flag,
	denied_withdrawn_flag
from 
	dl_analytics.mad_modelling_data_final_dbt;
	
-- Check grouping billing_period_frequency_code
select distinct
	billing_period_frequency_code_org,
	billing_period_frequency_code
from 
	dl_analytics.mad_modelling_data_final_dbt; 
	

-- check vehicle_age
select distinct 
--	claim_lodgement_date,
--	claim_loss_time_at_lodgement,
--	year_of_manufacture,
	vehicle_age
from 
	dl_analytics.mad_modelling_data_final_dbt
order by vehicle_age  asc; 


-- Check vehicle_car_park_type_code
select distinct
	vehicle_car_park_type_code_org,
	vehicle_car_park_type_code
from
	dl_analytics.mad_modelling_data_final_dbt;
	
-- Check agreed_to_market_value_ratio
select distinct 
	agreed_to_market_value_ratio
from
dl_analytics.mad_modelling_data_final_dbt;