



with all_values as(
  select distinct TASK_PRTY_TYP_NM as value_field
  from STAGING.STG_TASK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LOW','MEDIUM','HIGH','URGENT','OVERDUE'
        )
)

select count(*)
from validation_errors

