




with meet_condition as (

    select * from STAGING.DST_INVOICE_PROFILE where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(SUBROGATION_FLAG) = 1)

)

select count(*)
from validation_errors

