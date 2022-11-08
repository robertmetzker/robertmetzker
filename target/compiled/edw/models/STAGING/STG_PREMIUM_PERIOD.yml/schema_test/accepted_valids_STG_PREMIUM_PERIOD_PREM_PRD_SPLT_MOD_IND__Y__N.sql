



with all_values as(
  select distinct PREM_PRD_SPLT_MOD_IND as value_field
  from STAGING.STG_PREMIUM_PERIOD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

