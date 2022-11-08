



with all_values as(
  select distinct OUT_OF_OFFICE_IND as value_field
  from STAGING.DST_PLACE_OF_SERVICE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'N','Y'
        )
)

select count(*)
from validation_errors

