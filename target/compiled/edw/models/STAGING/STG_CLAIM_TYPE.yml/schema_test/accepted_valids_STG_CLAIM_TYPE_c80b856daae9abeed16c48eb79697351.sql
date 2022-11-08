



with all_values as(
  select distinct CLM_TYP_NM as value_field
  from STAGING.STG_CLAIM_TYPE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LOST TIME','MEDICAL ONLY','INCIDENT'
        )
)

select count(*)
from validation_errors

