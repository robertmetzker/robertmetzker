




with meet_condition as (

    select * from STAGING.DST_INJURED_WORKER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PHYSICAL_ADDRESS_STATE_CODE) = 2)

)

select count(*)
from validation_errors

