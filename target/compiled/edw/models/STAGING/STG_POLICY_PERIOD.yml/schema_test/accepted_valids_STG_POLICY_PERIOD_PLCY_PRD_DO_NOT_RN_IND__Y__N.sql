



with all_values as(
  select distinct PLCY_PRD_DO_NOT_RN_IND as value_field
  from STAGING.STG_POLICY_PERIOD 
  
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

