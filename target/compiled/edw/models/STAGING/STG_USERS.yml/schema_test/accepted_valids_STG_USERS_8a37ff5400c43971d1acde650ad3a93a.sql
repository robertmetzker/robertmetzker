



with all_values as(
  select distinct USER_TYP_CD as value_field
  from STAGING.STG_USERS 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CONV','EXTRN','INTRN','RSRC'
        )
)

select count(*)
from validation_errors

