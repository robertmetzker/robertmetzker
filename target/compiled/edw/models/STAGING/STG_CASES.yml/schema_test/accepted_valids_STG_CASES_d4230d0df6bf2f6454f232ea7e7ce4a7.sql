



with all_values as(
  select distinct CASE_PRTY_TYP_CD as value_field
  from STAGING.STG_CASES 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'HIGH','MDM','LOW','URGENT'
        )
)

select count(*)
from validation_errors

