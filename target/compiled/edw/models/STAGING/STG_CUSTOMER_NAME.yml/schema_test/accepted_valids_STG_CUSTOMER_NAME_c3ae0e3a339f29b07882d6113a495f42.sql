



with all_values as(
  select distinct CUST_NM_SFX_TYP_NM as value_field
  from STAGING.STG_CUSTOMER_NAME 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'JR','SR','I','II','III','IV','V'
        )
)

select count(*)
from validation_errors

