



with all_values as(
  select distinct DOCM_TYP_SRC_TYP_CD as value_field
  from STAGING.STG_DOCUMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CRR','OTHR_REG','REG','SYS','USR'
        )
)

select count(*)
from validation_errors

