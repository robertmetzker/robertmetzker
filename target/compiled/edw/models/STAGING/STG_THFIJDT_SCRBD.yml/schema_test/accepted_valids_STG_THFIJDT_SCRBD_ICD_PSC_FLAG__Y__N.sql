



with all_values as(
  select distinct ICD_PSC_FLAG as value_field
  from STAGING.STG_THFIJDT_SCRBD 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

