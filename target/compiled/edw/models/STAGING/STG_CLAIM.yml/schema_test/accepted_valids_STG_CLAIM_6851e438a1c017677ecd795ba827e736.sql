



with all_values as(
  select distinct NOI_TYP_CD as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACC','DTH_INST','DTH_SUB','DTH_SUB_OD','OCC_NONSTAT','OCC_STAT'
        )
)

select count(*)
from validation_errors

