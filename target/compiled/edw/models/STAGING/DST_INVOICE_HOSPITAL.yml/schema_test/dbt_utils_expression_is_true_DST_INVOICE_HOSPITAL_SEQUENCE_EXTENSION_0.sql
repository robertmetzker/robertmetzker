




with meet_condition as (

    select * from STAGING.DST_INVOICE_HOSPITAL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(SEQUENCE_EXTENSION >= 0)

)

select count(*)
from validation_errors

