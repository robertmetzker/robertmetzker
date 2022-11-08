




with meet_condition as (

    select * from STAGING.DST_EARNED_PREMIUM_BILLS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PLCY_NO) = 8)

)

select count(*)
from validation_errors

