



with all_values as(
  select distinct BATCH_TYPE as value_field
  from STAGING.DST_INVOICE_PRESCRIPTION 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'E','M'
        )
)

select count(*)
from validation_errors

