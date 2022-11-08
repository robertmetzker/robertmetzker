



with all_values as(
  select distinct GNDR_DGAT_CODE as value_field
  from STAGING.STG_THFICDP 
  
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

