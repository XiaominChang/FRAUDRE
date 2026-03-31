-- -------------------------------------------------------------------------
-- Author:      Xiaomin Chang                                                                              
-- Description: This view is used to store basic policy payment features 
--              for CTP modelling. Provided at the policy and payment date level.   
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                       DESCRIPTION                                                
-- 1.00     23/02/2024   Xiaomin Chang             Initial release        
-- -------------------------------------------------------------------------
{{ config(
          materialized='table',
          distributed_by = ['policy_identifier'],
          post_hook = grant_access(this)
          ) }}


WITH cte_policy_trans AS (
	select  DISTINCT
		policy_number_extended
	      , policy_id
	      , policy_product_code
	      , transaction_date
	      , to_date(transaction_date::text,'YYYYMMDD'):: date as pay_dt 
	      , lag(to_date(transaction_date::text,'YYYYMMDD')) over (partition by policy_id order by transaction_date)::date as last_pay_dt
		  , lead(to_date(transaction_date::text,'YYYYMMDD')) over (partition by policy_id order by transaction_date)::date as next_pay_dt
		  , lag(transaction_gross_amount) over (partition by policy_id order by transaction_date)as last_pay_amt
		  , lead(transaction_gross_amount) over (partition by policy_id order by transaction_date) as next_pay_amt
	      , payment_due_date
		  , transaction_gross_amount as pay_amt
		  , basic_premium_amount
		  , current_basic_premium_amount
	      , renewal_basic_premium_amount_item
	      , 'AUD' ::text as currency_cd
	      , transaction_type_code
	      , effective_from_date 
	      , effective_to_date 
	      , at_fault_collisions_in_the_last_2_yrs
	      , years_of_insurance_count
	      , years_of_membership
	      , policy_term_months 
	      , transaction_type_code
	from ctx.mv_huon_pi_policy_transaction_ctp_extn
	where 
			(effective_from_date >= policy_term_inception_date or effective_to_date > policy_term_inception_date)
			and (effective_from_date <= effective_to_date or row_effective_to_dttm is null) 
			and effective_from_date > 0
			and transaction_gross_amount>0 
			and policy_product_code='CTP'
			and transaction_type_code in ('0040','0200')
			and to_date(effective_from_date::text, 'YYYYMMDD') >= '{{ var("start_date") }}'::date - interval '1 year'
			and to_date(transaction_date::text,'YYYYMMDD')>='{{ var("start_date") }}'::date - interval '1 year'
),

cte_premium_info as (
	select  DISTINCT
		policy_id
		, transaction_date
		, -grossamt as grossamt
		, billing_account_number
		, docno
	from ctx.huon_pi_acctrn 
	where account_transaction_type_code ='CA' 
	and company_code=1
	and product_code='CTP' 
	and grossamt<0
	and to_date(transaction_date::text,'YYYYMMDD')>'{{ var("start_date") }}'::date - interval '1 year'
	union 
	select  DISTINCT
		policy_id
		, transaction_date
		, -grossamt as grossamt
		, billing_account_number
		, docno 
	from ctx.huon_pi_accths 
	where account_transaction_type_code ='CA' 
	and company_code=1
	and product_code='CTP'
	and grossamt<0
	and to_date(transaction_date::text,'YYYYMMDD')>'{{ var("start_date") }}'::date - interval '1 year'
),

cte_premium_trans AS (
	select distinct
		     cpt.*
--		   , case cpt.last_pay_dt + INTERVAL '7 month' > cpt.pay_dt
		   , cpi.billing_account_number
		   , cpi.docno	
	from cte_policy_trans cpt
	left join cte_premium_info cpi
	on  cpt.policy_id = cpi.policy_id 
		and cpt.transaction_date= cpi.transaction_date 
		and cpt.pay_amt - cpi.grossamt < 11
		and cpt.pay_amt- cpi.grossamt >=0
),


-- Financial account view from CTX
cte_clt_act as 
(select clt.client_id
	, clt.finclient_id
	, clt.account_number
	, clt.card_number
	, clt.card_type
	, clt.account_type
	, clt.account_sequence_number
	, clt.active_to_date
	, clt.effective_date
	, coalesce(lead(clt.effective_date) over(partition by clt.client_id, clt.account_sequence_number 
						order by effective_date, active_to_date, effective_time), clt.active_to_date) as effective_to
 from ctx.huon_pi_fincltact clt
 where clt.company = 1 
       and to_date(transaction_date::text,'YYYYMMDD')>'{{ var("start_date") }}'::date - interval '1 year'
),

-- Client role information from CTX
cte_clt_role as 
(	select intercode
		, client_id
		, active_from_date
		, coalesce(lead(active_from_date) 
			over(partition by client_id order by active_from_date, intercode desc, active_to_date), active_to_date) as effective_to_date
	from ctx.huon_pi_cltrole
	where role_type = 'FI'
	and company = 1
),

-- Preparation for auditable transactions 
-- This audit table is updated by each transation performed
cte_audtrl_pre as 
(	select  policy_id
		, policy_product_code
		, client_id
		, document_number
		, transaction_date
		, time_change_was_made 
	from ctx.huon_pi_audtrl
	where company_code = 1 
	and document_number > 0 
	and business_transaction_number = 0 
	and sequence = 0
	group by policy_id, policy_product_code, client_id, document_number, transaction_date, time_change_was_made
),

cte_audtrl as (
	select  policy_id
		, policy_product_code
		, client_id
		, document_number
		, transaction_date
		, row_number() over(partition by policy_id, policy_product_code, document_number, transaction_date 
		  order by time_change_was_made desc) rn
	from cte_audtrl_pre
	where  CASE WHEN LENGTH(transaction_date::text) = 8 THEN to_date(transaction_date::text, 'YYYYMMDD') ELSE NULL END >'{{ var("start_date") }}'::date - INTERVAL '1 year'
),

cte_premium_payment_summary as 
(
	select    'HUPI-'|| cpt.policy_number_extended as policy_identifier
		, 'HUPI-'||NULLIF(coalesce(act.client_id, aud.client_id),0) as policy_party_rk
		, cpt.policy_id as policy_id
		, cpt.pay_amt as payment_amt 
		, 'AUD' ::text as currency_cd
		, COALESCE(pmet.description1,(act.card_type ||act.account_type)) as payment_type_cv
		, cpt.pay_dt as payment_dt
		, case when clr.intercode is not null then replace(replace(btrim(clr.intercode),'-',''),'.','') else null end as bank_code
		, act.account_number as account_no
		, coalesce(act.card_number,rct.credit_card_number) as card_obfuscated_no
		, case when rct.credit_card_expiry_date <> 0 then rct.credit_card_expiry_date else null end as card_valid_to_dt
	from cte_premium_trans cpt
	left join ctx.huon_pi_bilact bilac
	on bilac.billing_account_number = cpt.billing_account_number
	and bilac.company_code = 1
	left join cte_clt_act act
	on act.client_id  = bilac.client_id
	and bilac.account_sequence_number = act.account_sequence_number
	and cpt.transaction_date < act.effective_to and cpt.transaction_date >= act.effective_date
	left join cte_clt_role clr
	on act.finclient_id = clr.client_id 
	and cpt.transaction_date < clr.effective_to_date and cpt.transaction_date >= clr.active_from_date
	left join ctx.huon_pi_rcthis rct
	on rct.receipt = cpt.docno 
	and rct.company = 1
	left join cte_audtrl aud
	on cpt.docno = aud.document_number
	and cpt.policy_id = aud.policy_id 
	and cpt.transaction_date = aud.transaction_date
	and aud.rn = 1
	left join ctx.huon_pi_litexpan pmet
	on (act.card_type||act.account_type) = pmet.keyvalue
	and pmet.recind = 800726
	-- where act.account_type is not null or act.card_type is not null
)

select  cpps.policy_identifier 
	  , cpps.policy_id
	  , cpps.policy_party_rk
	  , cpps.payment_amt
	  , cpps.currency_cd 
	  , cpps.payment_dt 
	  , lag(cpps.payment_dt) over (partition by cpps.policy_id, cpps.policy_party_rk order by cpps.payment_dt) as last_payment_dt
	  , lead(cpps.payment_dt) over (partition by cpps.policy_id, cpps.policy_party_rk order by cpps.payment_dt) as next_payment_dt 
	  , case when (lag(cpps.payment_dt) over (partition by cpps.policy_id, cpps.policy_party_rk order by cpps.payment_dt))
	        + INTERVAL '11 month'<= cpps.payment_dt then '1 year':: text
	    else '6 month' end as payment_frequency	  	
	  , convert_to(regexp_replace(regexp_replace(bref.mapping_value_2_code, '[\n\r]+','','g'),'[^[:print:]]','[\1]','g')
	      ,'win-1252') as payment_type_cd
	  , case when btrim(cpps.payment_type_cv) = '' then null 
	       else convert_to(regexp_replace(regexp_replace(cpps.payment_type_cv, '[\n\r]+','','g'),'[^[:print:]]','[\1]','g'),'win-1252')::text
	   	end payment_type_cv
	  , case when btrim(cpps.bank_code) = '' then null 
	       else convert_to(regexp_replace(regexp_replace(cpps.bank_code, '[\n\r]+','','g'),'[^[:print:]]','[\1]','g'),'win-1252')::text
	     end as bank_code
	  , case when btrim(cpps.account_no) = '' then null 
	       else convert_to(regexp_replace(regexp_replace(cpps.account_no, '[\n\r]+','','g'),'[^[:print:]]','[\1]','g'),'win-1252')::text
	    end account_no
	  , case when btrim(cpps.card_obfuscated_no) = '' then null
	       when btrim(cpps.card_obfuscated_no) =  '******' then null 
	       else convert_to(regexp_replace(regexp_replace(cpps.card_obfuscated_no, '[\n\r]+','','g'),'[^[:print:]]','[\1]','g'),'win-1252')::text
	    end as card_obfuscated_no
	  , cpps.card_valid_to_dt as card_valid_to_dt
from cte_premium_payment_summary cpps
left join bus_ref_data.ref_mapping bref
on btrim(upper(cpps.payment_type_cv)) = btrim(upper(bref.mapping_value_1_code))
and btrim(upper(bref.mapping_group_name)) = 'PAYMENT_TYPE_CD' 
and btrim(upper(bref.source_system_code))='SAS_FRAUD'

