



with all_values as(
  select distinct PREM_TYP_NM as value_field
  from STAGING.STG_PREMIUM_PERIOD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AUDITED','ESTIMATED','REPORTED'
        )
)

select count(*)
from validation_errors

