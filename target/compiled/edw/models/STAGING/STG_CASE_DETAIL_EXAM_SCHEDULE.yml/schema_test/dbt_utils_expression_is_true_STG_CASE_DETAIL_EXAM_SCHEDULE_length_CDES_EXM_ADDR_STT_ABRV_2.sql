




with meet_condition as (

    select * from STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(CDES_EXM_ADDR_STT_ABRV) = 2)

)

select count(*)
from validation_errors

