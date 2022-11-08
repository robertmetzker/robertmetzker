



with all_values as(
  select distinct DOCM_PPR_STK_TYP_CD as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'SPL_PPR','STND_PPR'
        )
)

select count(*)
from validation_errors

