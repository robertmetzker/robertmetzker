



with all_values as(
  select distinct K_PROGRAM_REASON_CODE as value_field
  from STAGING.DST_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'EMP_BL_ENDDT','EMP_WDR_DOI','RCH_LMT'
        )
)

select count(*)
from validation_errors

