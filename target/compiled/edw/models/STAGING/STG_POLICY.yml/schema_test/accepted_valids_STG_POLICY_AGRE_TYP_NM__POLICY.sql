



with all_values as(
  select distinct AGRE_TYP_NM as value_field
  from STAGING.STG_POLICY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'POLICY'
        )
)

select count(*)
from validation_errors

