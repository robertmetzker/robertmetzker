



with all_values as(
  select distinct CUST_BLK_ROLE_BLK_VOID_IND as value_field
  from STAGING.STG_CUSTOMER_BLOCK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'N','Y'
        )
)

select count(*)
from validation_errors

