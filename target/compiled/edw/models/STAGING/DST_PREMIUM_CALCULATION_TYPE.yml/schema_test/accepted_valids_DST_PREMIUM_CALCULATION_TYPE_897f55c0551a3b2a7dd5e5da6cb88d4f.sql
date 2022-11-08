



with all_values as(
  select distinct CURRENT_PREMIUM_CALCULATION_IND as value_field
  from STAGING.DST_PREMIUM_CALCULATION_TYPE 
  
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

