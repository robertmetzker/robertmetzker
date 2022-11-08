



with all_values as(
  select distinct EMPLOYER_PREMISES_IND as value_field
  from EDW_STAGING_DIM.DIM_CLAIM_DETAIL 
  
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

