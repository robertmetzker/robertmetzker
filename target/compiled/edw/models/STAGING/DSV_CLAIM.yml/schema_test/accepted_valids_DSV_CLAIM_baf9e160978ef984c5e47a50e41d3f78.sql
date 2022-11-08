



with all_values as(
  select distinct K_PROGRAM_ENROLLMENT_DESC as value_field
  from STAGING.DSV_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'YES','NO','UNKNOWN'
        )
)

select count(*)
from validation_errors

