



with all_values as(
  select distinct ENRL_STS_TYPE_CODE as value_field
  from STAGING.STG_PROVIDER_ENROLLMENT_STATUS 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACTV','DACTV','DENID','INPRO'
        )
)

select count(*)
from validation_errors

