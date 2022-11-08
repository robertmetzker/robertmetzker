



with all_values as(
  select distinct POLICY_TYPE_CODE as value_field
  from EDW_STAGING_DIM.DIM_YEAR_CONTROL_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PES','PEC','SI','BL','MIF','PA'
        )
)

select count(*)
from validation_errors

