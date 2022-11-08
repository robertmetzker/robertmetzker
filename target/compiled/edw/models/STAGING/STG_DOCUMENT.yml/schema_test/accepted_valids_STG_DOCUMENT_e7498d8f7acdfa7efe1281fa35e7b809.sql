



with all_values as(
  select distinct DOCM_STT_TYP_CD as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LOCKED','NON-PRDC','PRDC','VOID'
        )
)

select count(*)
from validation_errors

