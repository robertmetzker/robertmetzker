




with meet_condition as (

    select * from STAGING.STG_ACTIVITY_DETAIL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(ACTV_ID > 0)

)

select count(*)
from validation_errors

