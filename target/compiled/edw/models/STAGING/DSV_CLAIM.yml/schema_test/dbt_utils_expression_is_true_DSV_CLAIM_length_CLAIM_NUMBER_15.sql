




with meet_condition as (

    select * from STAGING.DSV_CLAIM where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(CLAIM_NUMBER) <= 15)

)

select count(*)
from validation_errors

