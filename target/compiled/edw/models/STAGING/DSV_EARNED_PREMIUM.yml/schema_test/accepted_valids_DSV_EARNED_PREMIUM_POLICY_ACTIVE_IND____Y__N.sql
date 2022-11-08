



with all_values as(
  select distinct POLICY_ACTIVE_IND as value_field
  from STAGING.DSV_EARNED_PREMIUM 
  
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

