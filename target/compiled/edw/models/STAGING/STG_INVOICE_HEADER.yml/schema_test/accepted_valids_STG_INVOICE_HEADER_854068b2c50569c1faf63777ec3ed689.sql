



with all_values as(
  select distinct INVOICE_STATUS as value_field
  from STAGING.STG_INVOICE_HEADER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AT','B','CD','CP','CT','D','P','PD','R','U','VD','VP','AA','ZZ'
        )
)

select count(*)
from validation_errors

