



with all_values as(
  select distinct BNFT_CYC_TYP_NM as value_field
  from STAGING.STG_INDEMNITY_PAYMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'FIXED','PERIODIC'
        )
)

select count(*)
from validation_errors

