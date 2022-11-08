



with all_values as(
  select distinct CLM_REL_SNPSHT_IND as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N','X'
        )
)

select count(*)
from validation_errors

