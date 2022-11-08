



with all_values as(
  select distinct CUST_ADDR_VLDT_PRFM_IND as value_field
  from STAGING.STG_CUSTOMER_ADDRESS 
  
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

