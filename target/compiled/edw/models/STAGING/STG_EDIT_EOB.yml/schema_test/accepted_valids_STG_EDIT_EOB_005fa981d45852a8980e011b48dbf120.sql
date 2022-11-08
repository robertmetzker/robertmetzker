



with all_values as(
  select distinct RELATIONSHIP_TYPE as value_field
  from STAGING.STG_EDIT_EOB 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'OV','FE','SE','SU','EO','OE'
        )
)

select count(*)
from validation_errors

