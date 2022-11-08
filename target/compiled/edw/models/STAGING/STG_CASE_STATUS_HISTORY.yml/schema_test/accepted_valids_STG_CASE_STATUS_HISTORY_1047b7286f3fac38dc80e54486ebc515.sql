



with all_values as(
  select distinct CASE_STT_TYP_CD as value_field
  from STAGING.STG_CASE_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'OPN','CLOS','INCOMP','INPROG'
        )
)

select count(*)
from validation_errors

