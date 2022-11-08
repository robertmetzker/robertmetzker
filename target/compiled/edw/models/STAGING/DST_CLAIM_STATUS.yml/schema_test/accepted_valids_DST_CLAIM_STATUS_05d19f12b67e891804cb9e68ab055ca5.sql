



with all_values as(
  select distinct CLM_STT_TYP_NM as value_field
  from STAGING.DST_CLAIM_STATUS 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'OPEN','CLOSED','INCOMPLETE'
        )
)

select count(*)
from validation_errors

