



with all_values as(
  select distinct LINE_STATUS_CODE as value_field
  from STAGING.DST_INVOICE_HOSPITAL 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'A','A1','A2','C','CD','CP','D','M','P','PD','R','U','VD','VP'
        )
)

select count(*)
from validation_errors

