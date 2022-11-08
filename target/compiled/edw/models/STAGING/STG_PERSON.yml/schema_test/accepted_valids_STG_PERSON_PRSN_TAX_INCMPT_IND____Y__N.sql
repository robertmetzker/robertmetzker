



with all_values as(
  select distinct PRSN_TAX_INCMPT_IND as value_field
  from STAGING.STG_PERSON 
  
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

