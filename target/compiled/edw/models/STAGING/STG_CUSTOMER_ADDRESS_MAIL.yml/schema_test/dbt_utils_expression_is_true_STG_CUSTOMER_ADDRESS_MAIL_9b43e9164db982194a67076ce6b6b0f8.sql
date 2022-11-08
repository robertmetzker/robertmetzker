




with meet_condition as (

    select * from STAGING.STG_CUSTOMER_ADDRESS_MAIL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(MAILING_ADDRESS_STATE_CODE) = 2)

)

select count(*)
from validation_errors

