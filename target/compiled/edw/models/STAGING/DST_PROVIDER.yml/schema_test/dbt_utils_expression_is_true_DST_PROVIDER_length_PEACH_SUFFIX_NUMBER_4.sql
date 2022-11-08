




with meet_condition as (

    select * from STAGING.DST_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PEACH_SUFFIX_NUMBER) = 4)

)

select count(*)
from validation_errors

