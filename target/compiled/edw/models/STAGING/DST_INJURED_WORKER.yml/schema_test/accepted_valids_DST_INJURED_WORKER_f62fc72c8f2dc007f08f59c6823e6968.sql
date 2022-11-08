



with all_values as(
  select distinct MAR_STS_TYP_CD as value_field
  from STAGING.DST_INJURED_WORKER 
  
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

