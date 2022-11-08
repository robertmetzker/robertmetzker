



with all_values as(
  select distinct MAR_STS_TYP_CD as value_field
  from STAGING.STG_PERSON_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'C','D','M','S','U','W','X'
        )
)

select count(*)
from validation_errors

