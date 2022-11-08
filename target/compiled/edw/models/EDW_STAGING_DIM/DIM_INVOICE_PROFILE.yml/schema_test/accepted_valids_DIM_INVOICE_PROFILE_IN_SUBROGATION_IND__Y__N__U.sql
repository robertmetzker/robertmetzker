



with all_values as(
  select distinct IN_SUBROGATION_IND as value_field
  from EDW_STAGING_DIM.DIM_INVOICE_PROFILE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N','U'
        )
)

select count(*)
from validation_errors

