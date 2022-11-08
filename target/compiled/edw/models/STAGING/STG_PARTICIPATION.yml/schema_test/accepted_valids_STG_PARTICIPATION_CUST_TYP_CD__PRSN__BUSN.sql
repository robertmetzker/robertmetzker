



with all_values as(
  select distinct CUST_TYP_CD as value_field
  from STAGING.STG_PARTICIPATION 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PRSN','BUSN'
        )
)

select count(*)
from validation_errors

