



with all_values as(
  select distinct ICD_STATUS_DESC as value_field
  from STAGING.DST_ICD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'VALID','INVALID'
        )
)

select count(*)
from validation_errors

