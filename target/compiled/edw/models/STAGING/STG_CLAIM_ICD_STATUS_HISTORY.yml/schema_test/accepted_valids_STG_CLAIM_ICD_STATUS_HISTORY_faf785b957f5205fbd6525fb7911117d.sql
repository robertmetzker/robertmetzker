



with all_values as(
  select distinct ICD_LOC_TYP_CD as value_field
  from STAGING.STG_CLAIM_ICD_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'BILATERAL','LEFT','RIGHT','CNV'
        )
)

select count(*)
from validation_errors

