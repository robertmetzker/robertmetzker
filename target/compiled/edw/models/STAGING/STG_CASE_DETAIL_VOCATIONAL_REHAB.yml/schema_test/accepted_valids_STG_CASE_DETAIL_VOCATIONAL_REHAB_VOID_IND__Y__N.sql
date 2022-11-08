



with all_values as(
  select distinct VOID_IND as value_field
  from STAGING.STG_CASE_DETAIL_VOCATIONAL_REHAB 
  
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

