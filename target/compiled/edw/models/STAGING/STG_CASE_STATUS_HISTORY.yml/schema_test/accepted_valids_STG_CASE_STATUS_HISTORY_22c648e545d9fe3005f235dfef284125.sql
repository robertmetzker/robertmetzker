



with all_values as(
  select distinct CASE_STT_TYP_NM as value_field
  from STAGING.STG_CASE_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'OPEN','CLOSED','INCOMPLETE','IN PROGRESS'
        )
)

select count(*)
from validation_errors

