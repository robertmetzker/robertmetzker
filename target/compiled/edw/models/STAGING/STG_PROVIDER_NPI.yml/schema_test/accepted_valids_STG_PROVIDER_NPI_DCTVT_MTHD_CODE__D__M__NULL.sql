



with all_values as(
  select distinct DCTVT_MTHD_CODE as value_field
  from STAGING.STG_PROVIDER_NPI 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'D','M','NULL'
        )
)

select count(*)
from validation_errors

