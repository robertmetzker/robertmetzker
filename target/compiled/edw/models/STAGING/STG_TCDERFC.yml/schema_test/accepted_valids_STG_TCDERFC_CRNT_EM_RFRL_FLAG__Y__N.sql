



with all_values as(
  select distinct CRNT_EM_RFRL_FLAG as value_field
  from STAGING.STG_TCDERFC 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

