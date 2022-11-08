




with meet_condition as (

    select * from STAGING.STG_POLICY_PERIOD_PREMIUM_DRV where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(PLCY_PRD_PREM_DRV_NO > 0)

)

select count(*)
from validation_errors

