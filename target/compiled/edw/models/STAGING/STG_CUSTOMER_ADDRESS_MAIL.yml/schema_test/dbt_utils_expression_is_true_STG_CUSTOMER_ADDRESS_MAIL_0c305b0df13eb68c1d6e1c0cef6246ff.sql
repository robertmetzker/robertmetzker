




with meet_condition as (

    select * from STAGING.STG_CUSTOMER_ADDRESS_MAIL where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(MAILING_FORMATTED_ADDRESS_ZIP4_CODE) = 4)

)

select count(*)
from validation_errors

