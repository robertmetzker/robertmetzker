



with all_values as(
  select distinct NPI_CNFRM_IND as value_field
  from STAGING.STG_PROVIDER_NPI 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'N','Y'
        )
)

select count(*)
from validation_errors

