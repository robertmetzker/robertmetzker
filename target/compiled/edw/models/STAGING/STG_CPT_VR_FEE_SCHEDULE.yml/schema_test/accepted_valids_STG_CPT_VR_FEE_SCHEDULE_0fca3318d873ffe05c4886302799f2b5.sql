



with all_values as(
  select distinct FEE_SCHEDULE as value_field
  from STAGING.STG_CPT_VR_FEE_SCHEDULE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'VOCATIONAL REHABILITATION'
        )
)

select count(*)
from validation_errors

