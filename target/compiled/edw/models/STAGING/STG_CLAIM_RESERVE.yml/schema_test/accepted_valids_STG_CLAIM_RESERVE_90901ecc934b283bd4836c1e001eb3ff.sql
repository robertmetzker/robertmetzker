



with all_values as(
  select distinct CLM_RSRV_SRC_TYP_NM as value_field
  from STAGING.STG_CLAIM_RESERVE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'RESERVE INTERFACE','SYSTEM SET','USER SET'
        )
)

select count(*)
from validation_errors

