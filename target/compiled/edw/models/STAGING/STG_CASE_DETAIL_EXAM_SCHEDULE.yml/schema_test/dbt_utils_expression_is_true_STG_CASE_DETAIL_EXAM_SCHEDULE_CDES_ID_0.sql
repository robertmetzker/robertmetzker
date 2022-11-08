




with meet_condition as (

    select * from STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CDES_ID > 0)

)

select count(*)
from validation_errors

