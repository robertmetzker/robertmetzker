




with meet_condition as (

    select * from STAGING.STG_INVOICE_HEADER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(MCO_NUMBER) = 5)

)

select count(*)
from validation_errors

