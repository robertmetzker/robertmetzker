




with meet_condition as (

    select * from STAGING.DST_INVOICE_HOSPITAL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(REVENUE_CENTER_CODE) = 4)

)

select count(*)
from validation_errors

