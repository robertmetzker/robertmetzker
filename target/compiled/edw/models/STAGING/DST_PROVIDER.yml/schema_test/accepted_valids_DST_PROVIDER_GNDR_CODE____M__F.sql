



with all_values as(
  select distinct GNDR_CODE as value_field
  from STAGING.DST_PROVIDER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'M','F'
        )
)

select count(*)
from validation_errors

