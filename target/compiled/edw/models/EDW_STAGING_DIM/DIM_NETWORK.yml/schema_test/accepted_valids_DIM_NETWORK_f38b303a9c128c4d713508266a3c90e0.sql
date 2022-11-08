



with all_values as(
  select distinct NETWORK_TYPE_DESC as value_field
  from EDW_STAGING_DIM.DIM_NETWORK 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PHARMACY BENEFIT MANAGER','QUALIFIED HEALTH PLAN','MANAGED CARE ORGANIZATION','THIRD PARTY','OHIO BUREAU OF WORKERS'' COMPENSATION','DIRECT PAY','UNKNOWN','INVALID','NA'
        )
)

select count(*)
from validation_errors

