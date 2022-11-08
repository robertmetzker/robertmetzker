



with all_values as(
  select distinct MARITAL_STATUS_TYPE_DESC as value_field
  from EDW_STAGING_DIM.DIM_INJURED_WORKER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'COMMON LAW','DIVORCED','MARRIED','SINGLE','UNKNOWN','WIDOWED','SEPARATED'
        )
)

select count(*)
from validation_errors

