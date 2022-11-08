



with all_values as(
  select distinct CURRENT_CORESUITE_CLAIM_TYPE_DESC as value_field
  from EDW_STAGING_DIM.DIM_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LOST TIME','MEDICAL ONLY','UNKNOWN'
        )
)

select count(*)
from validation_errors

