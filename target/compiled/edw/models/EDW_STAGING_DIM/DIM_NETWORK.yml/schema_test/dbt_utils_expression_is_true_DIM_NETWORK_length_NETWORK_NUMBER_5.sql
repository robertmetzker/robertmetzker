




with meet_condition as (

    select * from EDW_STAGING_DIM.DIM_NETWORK where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(NETWORK_NUMBER) = 5)

)

select count(*)
from validation_errors

