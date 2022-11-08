



with all_values as(
  select distinct SHR_TRM_RSN_TYP_CD as value_field
  from STAGING.STG_POLICY_PERIOD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ESTAB_CONCUR','REPL_BNDR','OTHR'
        )
)

select count(*)
from validation_errors

