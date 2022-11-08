




with meet_condition as (

    select * from EDW_STAGING_DIM.DIM_PROVIDER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(PEACH_BASE_NUMBER) = 7)

)

select count(*)
from validation_errors

