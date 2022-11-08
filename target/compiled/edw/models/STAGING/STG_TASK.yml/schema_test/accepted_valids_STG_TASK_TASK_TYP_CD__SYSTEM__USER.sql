



with all_values as(
  select distinct TASK_TYP_CD as value_field
  from STAGING.STG_TASK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'SYSTEM','USER'
        )
)

select count(*)
from validation_errors

