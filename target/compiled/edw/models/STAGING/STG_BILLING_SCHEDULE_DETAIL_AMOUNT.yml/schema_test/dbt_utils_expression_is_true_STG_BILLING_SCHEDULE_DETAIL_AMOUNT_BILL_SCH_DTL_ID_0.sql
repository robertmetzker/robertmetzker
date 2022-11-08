




with meet_condition as (

    select * from STAGING.STG_BILLING_SCHEDULE_DETAIL_AMOUNT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(BILL_SCH_DTL_ID > 0)

)

select count(*)
from validation_errors

