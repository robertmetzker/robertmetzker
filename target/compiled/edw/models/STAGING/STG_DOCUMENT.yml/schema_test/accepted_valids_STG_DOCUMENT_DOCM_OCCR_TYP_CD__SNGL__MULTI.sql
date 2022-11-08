



with all_values as(
  select distinct DOCM_OCCR_TYP_CD as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'SNGL','MULTI'
        )
)

select count(*)
from validation_errors

