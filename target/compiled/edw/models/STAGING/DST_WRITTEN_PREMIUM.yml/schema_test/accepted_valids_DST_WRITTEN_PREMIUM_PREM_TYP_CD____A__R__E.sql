



with all_values as(
  select distinct PREM_TYP_CD as value_field
  from STAGING.DST_WRITTEN_PREMIUM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'A','R','E'
        )
)

select count(*)
from validation_errors

