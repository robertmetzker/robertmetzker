



with all_values as(
  select distinct GNDR_TYP_NM as value_field
  from STAGING.STG_PERSON_HISTORY 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'FEMALE','MALE','UNKNOWN'
        )
)

select count(*)
from validation_errors

