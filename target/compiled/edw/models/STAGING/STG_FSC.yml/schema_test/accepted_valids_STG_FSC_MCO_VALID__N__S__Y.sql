



with all_values as(
  select distinct MCO_VALID as value_field
  from STAGING.STG_FSC 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'N','S','Y'
        )
)

select count(*)
from validation_errors

