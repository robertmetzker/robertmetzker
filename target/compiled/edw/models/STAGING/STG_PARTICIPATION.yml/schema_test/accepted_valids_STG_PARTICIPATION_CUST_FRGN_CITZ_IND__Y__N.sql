



with all_values as(
  select distinct CUST_FRGN_CITZ_IND as value_field
  from STAGING.STG_PARTICIPATION 
  
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

