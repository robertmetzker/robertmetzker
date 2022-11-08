



with all_values as(
  select distinct UNSL_CLM_TYP_CD as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        '250','270','280'
        )
)

select count(*)
from validation_errors

