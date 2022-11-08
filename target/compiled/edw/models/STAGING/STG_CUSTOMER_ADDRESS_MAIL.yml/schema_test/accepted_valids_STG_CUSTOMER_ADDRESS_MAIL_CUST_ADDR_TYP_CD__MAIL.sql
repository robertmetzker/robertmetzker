



with all_values as(
  select distinct CUST_ADDR_TYP_CD as value_field
  from STAGING.STG_CUSTOMER_ADDRESS_MAIL 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MAIL'
        )
)

select count(*)
from validation_errors

