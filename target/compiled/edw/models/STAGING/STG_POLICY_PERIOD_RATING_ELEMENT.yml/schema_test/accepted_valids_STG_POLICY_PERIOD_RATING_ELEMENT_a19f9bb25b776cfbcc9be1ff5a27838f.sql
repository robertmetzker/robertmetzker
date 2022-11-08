



with all_values as(
  select distinct RT_ELEM_DSPLY_TYP_NM as value_field
  from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'DATE','FACTOR','AMOUNT','PERCENTAGE FACTOR'
        )
)

select count(*)
from validation_errors

