



with all_values as(
  select distinct DOCM_STS_TYP_NM as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'COMPLETE','COMPLETE - PENDING','EMAILED','INCOMPLETE','INCOMPLETE - PENDING','PRODUCED','PRODUCED - SENT IN ERROR','PRODUCED - NOT SENT','VOID','N/A'
        )
)

select count(*)
from validation_errors

