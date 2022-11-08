



with all_values as(
  select distinct CLM_CTRPH_INJR_IND as value_field
  from STAGING.DST_CLAIM 
  
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

