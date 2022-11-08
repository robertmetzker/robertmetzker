



with all_values as(
  select distinct GENDER as value_field
  from STAGING.STG_ICD_PROCEDURE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'B','F','M'
        )
)

select count(*)
from validation_errors

