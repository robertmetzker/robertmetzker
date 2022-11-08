



with all_values as(
  select distinct APP_CNTX_TYP_CD as value_field
  from STAGING.STG_CASES 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CLAIM','POLICY','CUSTOMER'
        )
)

select count(*)
from validation_errors

