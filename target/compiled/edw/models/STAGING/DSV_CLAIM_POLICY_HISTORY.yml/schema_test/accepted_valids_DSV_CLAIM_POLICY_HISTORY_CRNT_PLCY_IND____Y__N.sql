



with all_values as(
  select distinct CRNT_PLCY_IND as value_field
  from STAGING.DSV_CLAIM_POLICY_HISTORY 
  
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

