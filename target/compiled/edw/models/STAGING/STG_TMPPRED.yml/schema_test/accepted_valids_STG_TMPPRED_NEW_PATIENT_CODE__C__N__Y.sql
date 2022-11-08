



with all_values as(
  select distinct NEW_PATIENT_CODE as value_field
  from STAGING.STG_TMPPRED 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'C','N','Y'
        )
)

select count(*)
from validation_errors

