



with all_values as(
  select distinct ICD_STS_TYP_CD as value_field
  from STAGING.STG_CLAIM_ICD_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACPT','ACCAPP','DENY','DENAPP','DISM','HEAR','PEND','STLB','STLIND','STLMED','SUBAGACCAPP','SUBAGNOTPAY','SUBAGPAY','SUBAGPNDABTMT','SUSPD'
        )
)

select count(*)
from validation_errors

