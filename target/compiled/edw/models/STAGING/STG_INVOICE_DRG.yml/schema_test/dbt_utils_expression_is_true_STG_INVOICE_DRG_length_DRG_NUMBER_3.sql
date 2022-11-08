




with meet_condition as (

    select * from STAGING.STG_INVOICE_DRG where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length( DRG_NUMBER ) = 3)

)

select count(*)
from validation_errors

