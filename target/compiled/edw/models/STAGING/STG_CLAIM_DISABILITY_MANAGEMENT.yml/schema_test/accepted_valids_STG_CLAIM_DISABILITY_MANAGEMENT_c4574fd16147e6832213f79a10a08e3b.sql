



with all_values as(
  select distinct CLM_DISAB_MANG_WK_STS_TYP_CD as value_field
  from STAGING.STG_CLAIM_DISABILITY_MANAGEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACT_RTW_SJSE','ACT_RTW_DJSE','ACT_RTW_SJDE','ACT_RTW_DJDE','ACT_RTW_JDU','NO_RTW','CONVERSION'
        )
)

select count(*)
from validation_errors

