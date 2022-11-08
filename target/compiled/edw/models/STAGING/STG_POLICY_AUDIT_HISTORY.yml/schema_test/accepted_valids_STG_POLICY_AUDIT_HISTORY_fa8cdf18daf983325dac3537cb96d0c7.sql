



with all_values as(
  select distinct PLCY_AUDT_TYP_CD as value_field
  from STAGING.STG_POLICY_AUDIT_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'EST','ETU','FLD','TU'
        )
)

select count(*)
from validation_errors

