



with all_values as(
  select distinct CUST_ROLE_TYP_CD as value_field
  from STAGING.STG_CUSTOMER_BLOCK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ACCT_HLDR','NONE','PROV'
        )
)

select count(*)
from validation_errors

