



with all_values as(
  select distinct TASK_STS_TYP_NM as value_field
  from STAGING.STG_TASK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'NEW','ACTIVE','PENDING','COMPLETED'
        )
)

select count(*)
from validation_errors

