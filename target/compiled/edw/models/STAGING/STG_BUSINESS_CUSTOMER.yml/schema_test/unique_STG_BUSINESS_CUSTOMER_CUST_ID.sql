
    
    



select count(*) as validation_errors
from (

    select
        CUST_ID

    from STAGING.STG_BUSINESS_CUSTOMER
    where CUST_ID is not null
    group by CUST_ID
    having count(*) > 1

) validation_errors


