



with all_values as(
  select distinct NPI_CONFIRMATION_IND as value_field
  from STAGING.DSV_PROVIDER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'N','Y'
        )
)

select count(*)
from validation_errors

