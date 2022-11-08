



with all_values as(
  select distinct MAR_STS_TYP_NM as value_field
  from STAGING.DST_INJURED_WORKER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'COMMON LAW','DIVORCED','MARRIED','SINGLE','UNKNOWN','WIDOWED','SEPARATED'
        )
)

select count(*)
from validation_errors

