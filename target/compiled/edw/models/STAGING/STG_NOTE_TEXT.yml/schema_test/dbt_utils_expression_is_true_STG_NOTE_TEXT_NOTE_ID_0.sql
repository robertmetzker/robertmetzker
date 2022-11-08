




with meet_condition as (

    select * from STAGING.STG_NOTE_TEXT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(NOTE_ID > 0)

)

select count(*)
from validation_errors

