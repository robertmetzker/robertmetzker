



with all_values as(
  select distinct CLM_RSRV_SRC_TYP_CD as value_field
  from STAGING.STG_CLAIM_RESERVE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MIRA','SYS','USR'
        )
)

select count(*)
from validation_errors

