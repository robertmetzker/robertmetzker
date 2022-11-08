



with all_values as(
  select distinct ICD_VER_CD as value_field
  from STAGING.STG_CLAIM_ICD_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        '9','10'
        )
)

select count(*)
from validation_errors

