



with all_values as(
  select distinct NOI_CTG_TYP_CD as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACC','CUM_INJ','DTH','OCC_DIS','SPEC_INJ'
        )
)

select count(*)
from validation_errors

