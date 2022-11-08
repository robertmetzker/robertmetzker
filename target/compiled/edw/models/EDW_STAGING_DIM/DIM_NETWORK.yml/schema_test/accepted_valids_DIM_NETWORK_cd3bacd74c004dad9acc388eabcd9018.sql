



with all_values as(
  select distinct NETWORK_TYPE_CODE as value_field
  from EDW_STAGING_DIM.DIM_NETWORK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MCO','PBM','BWC','TDP','UNK','INV','NA'
        )
)

select count(*)
from validation_errors

