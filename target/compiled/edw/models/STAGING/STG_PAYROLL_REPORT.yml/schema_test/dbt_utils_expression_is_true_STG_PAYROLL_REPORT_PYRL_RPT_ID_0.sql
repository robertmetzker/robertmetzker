




with meet_condition as (

    select * from STAGING.STG_PAYROLL_REPORT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(PYRL_RPT_ID > 0)

)

select count(*)
from validation_errors

