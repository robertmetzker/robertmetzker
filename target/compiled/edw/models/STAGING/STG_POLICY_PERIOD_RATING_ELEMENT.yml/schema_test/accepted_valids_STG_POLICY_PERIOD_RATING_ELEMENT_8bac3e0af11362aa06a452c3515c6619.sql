



with all_values as(
  select distinct RT_ELEM_DSPLY_TYP_CD as value_field
  from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'DT','FCTR','NO','PCT'
        )
)

select count(*)
from validation_errors

