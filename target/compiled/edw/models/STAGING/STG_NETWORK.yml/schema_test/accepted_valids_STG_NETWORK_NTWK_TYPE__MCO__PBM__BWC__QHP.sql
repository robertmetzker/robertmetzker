



with all_values as(
  select distinct NTWK_TYPE as value_field
  from STAGING.STG_NETWORK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MCO','PBM','BWC','QHP'
        )
)

select count(*)
from validation_errors

