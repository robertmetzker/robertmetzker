



with all_values as(
  select distinct RELATIONSHIP_TYPE_DESC as value_field
  from STAGING.STG_EDIT_EOB 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'OVERRIDE EOB','FIRE EDIT','SET EOB','EDIT FIRED WHEN EOB IS USED AS OVERRIDE'
        )
)

select count(*)
from validation_errors

