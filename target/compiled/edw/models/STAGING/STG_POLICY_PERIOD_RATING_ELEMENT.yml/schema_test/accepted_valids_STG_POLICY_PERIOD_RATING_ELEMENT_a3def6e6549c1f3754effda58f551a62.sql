



with all_values as(
  select distinct WC_CLS_RT_TIER_TYP_NM as value_field
  from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'TIER 1 DEVIATION','TIER 2 DEVIATION'
        )
)

select count(*)
from validation_errors

