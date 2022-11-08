



with all_values as(
  select distinct PRSN_IND as value_field
  from STAGING.DST_PROVIDER 
  
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

