
    
    



select count(*) as validation_errors
from (

    select
        CUST_ADDR_ID

    from STAGING.STG_CUSTOMER_ADDRESS
    where CUST_ADDR_ID is not null
    group by CUST_ADDR_ID
    having count(*) > 1

) validation_errors


