



with all_values as(
  select distinct CURRENT_IND as value_field
  from STAGING.DST_INVOICE_STATUS 
  
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
