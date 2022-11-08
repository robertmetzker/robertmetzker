



with all_values as(
  select distinct REPORTING_FREQUENCY_TYPE_CODE as value_field
  from STAGING.DST_EARNED_PREMIUM_BILLS 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AN','MO','AN_DEP','SA','QTR','N/A'
        )
)

select count(*)
from validation_errors

