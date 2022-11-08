




with meet_condition as (

    select * from STAGING.STG_ASSIGNMENT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(USER_ID >= -10)

)

select count(*)
from validation_errors

