



with all_values as(
  select distinct CURRENT_CERTIFICATION_STATUS_DESC as value_field
  from STAGING.DST_PROVIDER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CERTIFIED','DECERTIFIED','DENIED','LAPSED','IN PROCESS','PENDING DECERTIFICATION'
        )
)

select count(*)
from validation_errors

