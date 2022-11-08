



with all_values as(
  select distinct DEP_SERVICE_CODE as value_field
  from STAGING.DST_PROVIDER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'A','B','F','R'
        )
)

select count(*)
from validation_errors

