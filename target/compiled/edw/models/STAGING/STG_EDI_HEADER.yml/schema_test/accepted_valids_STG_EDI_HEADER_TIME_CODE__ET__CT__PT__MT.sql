



with all_values as(
  select distinct TIME_CODE as value_field
  from STAGING.STG_EDI_HEADER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ET','CT','PT','MT'
        )
)

select count(*)
from validation_errors

