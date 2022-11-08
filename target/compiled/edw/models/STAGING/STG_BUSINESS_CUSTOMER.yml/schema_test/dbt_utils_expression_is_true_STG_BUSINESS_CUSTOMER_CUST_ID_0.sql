




with meet_condition as (

    select * from STAGING.STG_BUSINESS_CUSTOMER where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CUST_ID > 0)

)

select count(*)
from validation_errors

