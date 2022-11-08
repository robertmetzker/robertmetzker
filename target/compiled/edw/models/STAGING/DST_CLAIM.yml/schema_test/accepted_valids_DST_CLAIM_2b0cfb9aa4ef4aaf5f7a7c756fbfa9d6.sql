



with all_values as(
  select distinct CLM_TYP_NM as value_field
  from STAGING.DST_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LOST TIME','MEDICAL ONLY'
        )
)

select count(*)
from validation_errors

