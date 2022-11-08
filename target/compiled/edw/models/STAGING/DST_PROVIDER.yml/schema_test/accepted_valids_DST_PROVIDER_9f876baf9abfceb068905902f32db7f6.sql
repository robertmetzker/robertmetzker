



with all_values as(
  select distinct CURRENT_ENROLLMENT_STATUS_DESC as value_field
  from STAGING.DST_PROVIDER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACTIVE','DEACTIVATED','DENIED','IN PROCESS'
        )
)

select count(*)
from validation_errors

