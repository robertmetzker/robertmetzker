



with all_values as(
  select distinct DOCM_STS_TYP_CD as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'COMP','COMP_PND','EMAIL','INCOMP','INCOMP_PND','PRDC','PRDC_IN_ERR','PRDC_NOT_SENT','VOID'
        )
)

select count(*)
from validation_errors

