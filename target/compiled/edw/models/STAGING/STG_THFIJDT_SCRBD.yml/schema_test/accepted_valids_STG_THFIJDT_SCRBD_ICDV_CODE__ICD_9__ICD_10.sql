



with all_values as(
  select distinct ICDV_CODE as value_field
  from STAGING.STG_THFIJDT_SCRBD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ICD-9','ICD-10'
        )
)

select count(*)
from validation_errors

