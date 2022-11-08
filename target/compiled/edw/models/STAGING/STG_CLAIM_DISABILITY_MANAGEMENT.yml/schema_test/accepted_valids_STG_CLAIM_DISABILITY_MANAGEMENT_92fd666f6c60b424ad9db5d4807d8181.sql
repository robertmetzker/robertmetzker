



with all_values as(
  select distinct CLM_DISAB_MANG_MED_STS_TYP_CD as value_field
  from STAGING.STG_CLAIM_DISABILITY_MANAGEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CONVERSION','NOT_REL','ARTW_FULL_DUTY','ARTW_WTH_REST'
        )
)

select count(*)
from validation_errors

