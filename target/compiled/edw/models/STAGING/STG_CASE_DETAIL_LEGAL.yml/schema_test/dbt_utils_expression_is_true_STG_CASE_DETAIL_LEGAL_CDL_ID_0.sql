




with meet_condition as (

    select * from STAGING.STG_CASE_DETAIL_LEGAL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CDL_ID > 0)

)

select count(*)
from validation_errors

