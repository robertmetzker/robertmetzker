



with all_values as(
  select distinct OBLC_BDY_LCTN_CODE as value_field
  from STAGING.STG_THFIJDT_SCRBD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'B','L','R'
        )
)

select count(*)
from validation_errors

