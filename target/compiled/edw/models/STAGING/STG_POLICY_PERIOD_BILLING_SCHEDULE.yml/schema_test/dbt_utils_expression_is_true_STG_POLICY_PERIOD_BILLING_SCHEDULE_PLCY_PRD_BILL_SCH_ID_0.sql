




with meet_condition as (

    select * from STAGING.STG_POLICY_PERIOD_BILLING_SCHEDULE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(PLCY_PRD_BILL_SCH_ID > 0)

)

select count(*)
from validation_errors

