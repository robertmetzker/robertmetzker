




with meet_condition as (

    select * from STAGING.STG_CUSTOMER_CHILD_SUPPORT where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(CUST_ID_PRSN > 0)

)

select count(*)
from validation_errors

