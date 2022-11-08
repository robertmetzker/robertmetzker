




with meet_condition as (

    select * from STAGING.STG_FSC where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(GPI) = 14)

)

select count(*)
from validation_errors

