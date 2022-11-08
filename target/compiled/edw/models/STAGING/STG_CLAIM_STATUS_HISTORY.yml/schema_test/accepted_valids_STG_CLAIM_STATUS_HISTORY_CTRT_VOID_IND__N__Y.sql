



with all_values as(
  select distinct CTRT_VOID_IND as value_field
  from STAGING.STG_CLAIM_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'N','Y'
        )
)

select count(*)
from validation_errors

