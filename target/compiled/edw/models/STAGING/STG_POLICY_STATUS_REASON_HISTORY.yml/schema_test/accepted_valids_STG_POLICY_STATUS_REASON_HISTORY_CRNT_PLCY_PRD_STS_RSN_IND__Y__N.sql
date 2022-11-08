



with all_values as(
  select distinct CRNT_PLCY_PRD_STS_RSN_IND as value_field
  from STAGING.STG_POLICY_STATUS_REASON_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

