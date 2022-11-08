



with all_values as(
  select distinct DEP_SRVC_CODE as value_field
  from STAGING.STG_PROVIDER_DEP_SERVICE 
  
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

