



with all_values as(
  select distinct INPUT_METHOD_DESC as value_field
  from STAGING.DST_INVOICE_PROFILE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'SCANNED','MANUAL','ELECTRONIC DATA INTERFACE'
        )
)

select count(*)
from validation_errors

