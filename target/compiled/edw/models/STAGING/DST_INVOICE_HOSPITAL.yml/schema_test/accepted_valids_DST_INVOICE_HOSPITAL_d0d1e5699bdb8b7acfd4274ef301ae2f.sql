



with all_values as(
  select distinct INVOICE_STATUS as value_field
  from STAGING.DST_INVOICE_HOSPITAL 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AT','B','CD','CP','CT','D','P','PD','R','U','VD','VP','ZZ','AA'
        )
)

select count(*)
from validation_errors

