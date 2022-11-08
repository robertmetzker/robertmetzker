




with meet_condition as (

    select * from STAGING.STG_TASK where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(TASK_ID > 0)

)

select count(*)
from validation_errors

