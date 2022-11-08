



with all_values as(
  select distinct ICD_STATUS_CODE as value_field
  from STAGING.DST_ICD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'V','I'
        )
)

select count(*)
from validation_errors

