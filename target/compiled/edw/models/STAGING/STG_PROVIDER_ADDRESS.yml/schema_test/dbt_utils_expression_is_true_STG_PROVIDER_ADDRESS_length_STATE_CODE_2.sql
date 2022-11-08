




with meet_condition as (

    select * from STAGING.STG_PROVIDER_ADDRESS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(STATE_CODE) = 2)

)

select count(*)
from validation_errors

