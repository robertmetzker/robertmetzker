



with all_values as(
  select distinct PLCY_BLK_HDR_DSPLY_IND as value_field
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

