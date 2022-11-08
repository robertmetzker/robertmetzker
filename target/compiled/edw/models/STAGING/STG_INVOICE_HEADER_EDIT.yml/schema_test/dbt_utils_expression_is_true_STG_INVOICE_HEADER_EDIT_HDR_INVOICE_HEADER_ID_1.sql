




with meet_condition as (

    select * from STAGING.STG_INVOICE_HEADER_EDIT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(HDR_INVOICE_HEADER_ID >= 1)

)

select count(*)
from validation_errors

