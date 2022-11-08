




with meet_condition as (

    select * from STAGING.DST_REVENUE_CODE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(LENGTH(SERVICE_CODE) = 4)

)

select count(*)
from validation_errors

