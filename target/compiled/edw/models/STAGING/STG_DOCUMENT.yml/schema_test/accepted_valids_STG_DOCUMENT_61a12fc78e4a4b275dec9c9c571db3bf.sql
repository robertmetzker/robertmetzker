



with all_values as(
  select distinct DOCM_TYP_SRC_TYP_NM as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CARRIER','OTHER REGULATORY','REGULATOR','SYSTEM','USER'
        )
)

select count(*)
from validation_errors

