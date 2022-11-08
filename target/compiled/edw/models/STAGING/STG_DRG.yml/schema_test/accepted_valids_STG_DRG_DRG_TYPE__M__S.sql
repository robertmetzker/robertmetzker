



with all_values as(
  select distinct DRG_TYPE as value_field
  from STAGING.STG_DRG 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'M','S'
        )
)

select count(*)
from validation_errors

