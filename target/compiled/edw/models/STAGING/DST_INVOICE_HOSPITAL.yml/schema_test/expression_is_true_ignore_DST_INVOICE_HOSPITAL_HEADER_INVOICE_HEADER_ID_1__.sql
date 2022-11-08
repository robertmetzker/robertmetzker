





with meet_condition as (

    select * from STAGING.DST_INVOICE_HOSPITAL  
    
),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(HEADER_INVOICE_HEADER_ID >= 1)

)

select count(*)
from validation_errors

