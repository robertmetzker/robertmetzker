



with all_values as(
  select distinct ENRL_STS_NAME as value_field
  from STAGING.STG_PROVIDER_ENROLLMENT_STATUS 
  
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

