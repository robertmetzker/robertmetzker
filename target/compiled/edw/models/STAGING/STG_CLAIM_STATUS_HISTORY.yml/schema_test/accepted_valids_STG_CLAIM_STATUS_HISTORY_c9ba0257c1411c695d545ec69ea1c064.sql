



with all_values as(
  select distinct CLM_STT_TYP_NM as value_field
  from STAGING.STG_CLAIM_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CLOSED','INCOMPLETE','OPEN'
        )
)

select count(*)
from validation_errors

