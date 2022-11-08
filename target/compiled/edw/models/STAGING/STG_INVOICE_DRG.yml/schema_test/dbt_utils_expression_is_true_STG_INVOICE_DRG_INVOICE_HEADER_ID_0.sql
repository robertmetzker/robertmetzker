




with meet_condition as (

    select * from STAGING.STG_INVOICE_DRG where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(INVOICE_HEADER_ID > 0)

)

select count(*)
from validation_errors

