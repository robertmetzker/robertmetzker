




with meet_condition as (

    select * from STAGING.STG_INVOICE_LINE_EOB where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(INVOICE_NUMBER) = 21)

)

select count(*)
from validation_errors

