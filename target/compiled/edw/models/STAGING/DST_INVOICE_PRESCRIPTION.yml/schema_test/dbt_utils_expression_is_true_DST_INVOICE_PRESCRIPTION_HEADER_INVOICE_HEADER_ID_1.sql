




with meet_condition as (

    select * from STAGING.DST_INVOICE_PRESCRIPTION where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(HEADER_INVOICE_HEADER_ID >= 1)

)

select count(*)
from validation_errors

