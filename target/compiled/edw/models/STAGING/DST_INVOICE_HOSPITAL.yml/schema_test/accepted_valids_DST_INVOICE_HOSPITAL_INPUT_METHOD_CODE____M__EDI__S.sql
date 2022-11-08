



with all_values as(
  select distinct INPUT_METHOD_CODE as value_field
  from STAGING.DST_INVOICE_HOSPITAL 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'M','EDI','S'
        )
)

select count(*)
from validation_errors

