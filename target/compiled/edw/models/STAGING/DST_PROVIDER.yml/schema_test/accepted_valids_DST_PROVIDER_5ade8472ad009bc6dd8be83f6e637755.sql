



with all_values as(
  select distinct OUT_OF_STATE_STATUS_DESC as value_field
  from STAGING.DST_PROVIDER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'OUT OF STATE','BORDERING STATE','IN STATE'
        )
)

select count(*)
from validation_errors

