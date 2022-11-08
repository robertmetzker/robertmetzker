




with meet_condition as (

    select * from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(PPRE_RT >= 0)

)

select count(*)
from validation_errors

