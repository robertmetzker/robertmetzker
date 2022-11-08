



with all_values as(
  select distinct CMS_PROHIBITED_IND as value_field
  from EDW_STAGING_DIM.DIM_ICD 
  
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

