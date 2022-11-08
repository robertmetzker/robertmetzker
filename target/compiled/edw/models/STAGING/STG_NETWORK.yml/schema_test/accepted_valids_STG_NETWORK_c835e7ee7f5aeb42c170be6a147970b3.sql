



with all_values as(
  select distinct REF_DSC as value_field
  from STAGING.STG_NETWORK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'DIRECT PAY','MANAGED CARE ORGANIZATION','PHARMACEUTICAL','QUALIFIED HEALTH PLAN'
        )
)

select count(*)
from validation_errors

