



with all_values as(
  select distinct AGRE_TYP_CD as value_field
  from STAGING.STG_POLICY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PLCY'
        )
)

select count(*)
from validation_errors

