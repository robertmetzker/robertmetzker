



with all_values as(
  select distinct RT_TYP_NM as value_field
  from STAGING.DST_WRITTEN_PREMIUM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'EMPLOYEE','INCLUDED INDIVIDUAL'
        )
)

select count(*)
from validation_errors

