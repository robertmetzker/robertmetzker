



with all_values as(
  select distinct REPORTED_ZERO_PAYROLL_IND as value_field
  from STAGING.DST_EARNED_PREMIUM_BILLS 
  
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

