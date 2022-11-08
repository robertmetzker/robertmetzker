




with meet_condition as (

    select * from STAGING.DST_PROVIDER_ENROLLMENT_STATUS_LOG where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PEACH_NUMBER) = 11)

)

select count(*)
from validation_errors

