



with all_values as(
  select distinct DOCM_PPR_STK_TYP_NM as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'SPECIAL PAPER','STANDARD PAPER'
        )
)

select count(*)
from validation_errors

