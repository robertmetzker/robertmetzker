



with all_values as(
  select distinct CUST_NM_TTL_TYP_NM as value_field
  from STAGING.STG_CUSTOMER_NAME 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MR.','MS.','MRS.','DR.','FR.','SR.'
        )
)

select count(*)
from validation_errors

