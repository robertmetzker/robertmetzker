



with all_values as(
  select distinct THREATENING_BEHAVIOR_BLOCK_IND as value_field
  from EDW_STAGING_DIM.DIM_INJURED_WORKER 
  
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

