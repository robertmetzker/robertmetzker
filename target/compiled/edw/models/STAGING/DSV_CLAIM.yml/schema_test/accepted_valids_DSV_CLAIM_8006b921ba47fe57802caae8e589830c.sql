



with all_values as(
  select distinct COVID_HEALTH_CARE_WORKER_IND as value_field
  from STAGING.DSV_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N','U'
        )
)

select count(*)
from validation_errors

