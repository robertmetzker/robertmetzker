



with all_values as(
  select distinct CLAIM_STATE_DESC as value_field
  from STAGING.DSV_CLAIM_STATUS 
  
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

