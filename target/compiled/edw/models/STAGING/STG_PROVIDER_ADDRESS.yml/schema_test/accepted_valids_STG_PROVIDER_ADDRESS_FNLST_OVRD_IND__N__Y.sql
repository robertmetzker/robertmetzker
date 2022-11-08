



with all_values as(
  select distinct FNLST_OVRD_IND as value_field
  from STAGING.STG_PROVIDER_ADDRESS 
  
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

