



with all_values as(
  select distinct DOCM_TYP_VER_CREA_EXCP_IND as value_field
  from STAGING.STG_DOCUMENT 
  
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

