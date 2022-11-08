



with all_values as(
  select distinct GENDER_TYPE_CODE as value_field
  from STAGING.DST_ICD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'M','F','B'
        )
)

select count(*)
from validation_errors

