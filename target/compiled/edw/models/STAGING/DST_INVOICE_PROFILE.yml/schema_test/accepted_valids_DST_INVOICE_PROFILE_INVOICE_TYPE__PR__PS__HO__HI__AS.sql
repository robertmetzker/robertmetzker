



with all_values as(
  select distinct INVOICE_TYPE as value_field
  from STAGING.DST_INVOICE_PROFILE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PR','PS','HO','HI','AS'
        )
)

select count(*)
from validation_errors

