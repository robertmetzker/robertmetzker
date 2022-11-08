




with meet_condition as (

    select * from STAGING.DST_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PEACH_FORMATTED_NUMBER) = 13)

)

select count(*)
from validation_errors

