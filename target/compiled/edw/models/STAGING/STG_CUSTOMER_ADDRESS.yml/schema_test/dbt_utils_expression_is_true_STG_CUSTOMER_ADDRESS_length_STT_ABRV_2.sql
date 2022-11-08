




with meet_condition as (

    select * from STAGING.STG_CUSTOMER_ADDRESS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length( STT_ABRV ) = 2)

)

select count(*)
from validation_errors

