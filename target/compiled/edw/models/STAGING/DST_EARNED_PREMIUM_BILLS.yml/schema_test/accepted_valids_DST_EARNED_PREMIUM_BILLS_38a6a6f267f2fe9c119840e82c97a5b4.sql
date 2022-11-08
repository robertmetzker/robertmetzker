



with all_values as(
  select distinct REPORTING_FREQUENCY_TYPE_DESC as value_field
  from STAGING.DST_EARNED_PREMIUM_BILLS 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PAYMENT PLAN','MONTHLY','ANNUAL','SEMI-ANNUAL','QUARTERLY','NO REPORTING FREQUENCY'
        )
)

select count(*)
from validation_errors

