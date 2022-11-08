



with all_values as(
  select distinct CMMERT_NM as value_field
  from STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'APS','OTHER'
        )
)

select count(*)
from validation_errors

