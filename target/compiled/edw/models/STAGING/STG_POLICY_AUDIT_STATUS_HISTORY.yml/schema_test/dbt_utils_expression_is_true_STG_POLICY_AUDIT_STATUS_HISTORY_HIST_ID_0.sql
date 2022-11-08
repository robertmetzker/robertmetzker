




with meet_condition as (

    select * from STAGING.STG_POLICY_AUDIT_STATUS_HISTORY where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(HIST_ID > 0)

)

select count(*)
from validation_errors

