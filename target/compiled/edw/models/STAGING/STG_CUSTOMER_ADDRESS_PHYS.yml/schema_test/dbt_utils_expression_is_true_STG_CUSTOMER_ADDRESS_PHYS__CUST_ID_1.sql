




with meet_condition as (

    select * from STAGING.STG_CUSTOMER_ADDRESS_PHYS where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not((CUST_ID) >= 1)

)

select count(*)
from validation_errors

