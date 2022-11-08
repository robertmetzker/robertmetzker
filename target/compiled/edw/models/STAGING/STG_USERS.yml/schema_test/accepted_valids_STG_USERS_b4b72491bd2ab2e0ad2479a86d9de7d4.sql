



with all_values as(
  select distinct USER_TYP_NM as value_field
  from STAGING.STG_USERS 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CONVERSION','EXTERNAL','INTERNAL','RESOURCE'
        )
)

select count(*)
from validation_errors

