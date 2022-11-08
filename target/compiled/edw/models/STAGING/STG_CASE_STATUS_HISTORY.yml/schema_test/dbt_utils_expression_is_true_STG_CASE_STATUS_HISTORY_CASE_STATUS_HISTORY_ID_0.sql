




with meet_condition as (

    select * from STAGING.STG_CASE_STATUS_HISTORY where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CASE_STATUS_HISTORY_ID > 0)

)

select count(*)
from validation_errors

