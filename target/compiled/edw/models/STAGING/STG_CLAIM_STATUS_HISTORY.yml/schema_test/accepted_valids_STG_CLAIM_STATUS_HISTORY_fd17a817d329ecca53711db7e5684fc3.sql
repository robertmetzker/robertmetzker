



with all_values as(
  select distinct CLM_STS_TYP_CD as value_field
  from STAGING.STG_CLAIM_STATUS_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACPT','DNY','EXPROCCR','INCOMPOCCR','PND','PNDCONDBNFT','PNDCOV','SBMT'
        )
)

select count(*)
from validation_errors

