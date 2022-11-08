



with all_values as(
  select distinct TAX_FILE_STS_TYP_NM as value_field
  from STAGING.STG_PERSON 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'UNKNOWN'
        )
)

select count(*)
from validation_errors

