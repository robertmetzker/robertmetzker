



with all_values as(
  select distinct CASE_ISS_PRI_IND as value_field
  from STAGING.STG_CASE_ISSUE 
  
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

