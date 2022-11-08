



with all_values as(
  select distinct INVOICE_TYPE as value_field
  from STAGING.STG_INVOICE_HEADER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AS','HI','HO','PR','PS'
        )
)

select count(*)
from validation_errors

