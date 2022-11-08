



with all_values as(
  select distinct RT_ELEM_LMCJ_TYP_WEB_DSPLY_IND as value_field
  from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

