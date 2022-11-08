



with all_values as(
  select distinct PPAD_UNPRD_PRCS_IND as value_field
  from STAGING.STG_POLICY_AUDIT_HISTORY 
  
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

