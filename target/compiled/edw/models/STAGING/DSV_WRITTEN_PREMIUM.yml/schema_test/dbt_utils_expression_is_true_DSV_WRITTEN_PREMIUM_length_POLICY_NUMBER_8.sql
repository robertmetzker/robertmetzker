




with meet_condition as (

    select * from STAGING.DSV_WRITTEN_PREMIUM where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(POLICY_NUMBER) = 8)

)

select count(*)
from validation_errors

