



with all_values as(
  select distinct AUTHORIZED_CLAIM_USE_IND as value_field
  from STAGING.DSV_ICD 
  
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

