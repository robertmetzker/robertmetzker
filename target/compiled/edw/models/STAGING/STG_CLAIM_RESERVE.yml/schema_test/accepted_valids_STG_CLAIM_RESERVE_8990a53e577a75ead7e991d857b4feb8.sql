



with all_values as(
  select distinct CLM_RSRV_STS_TYP_NM as value_field
  from STAGING.STG_CLAIM_RESERVE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACCEPTED','PENDING','PENDING AUTHORIZATION','REJECTED'
        )
)

select count(*)
from validation_errors

