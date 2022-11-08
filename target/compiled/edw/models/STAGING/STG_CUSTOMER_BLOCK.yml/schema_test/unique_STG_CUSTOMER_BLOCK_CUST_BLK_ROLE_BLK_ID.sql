
    
    



select count(*) as validation_errors
from (

    select
        CUST_BLK_ROLE_BLK_ID

    from STAGING.STG_CUSTOMER_BLOCK
    where CUST_BLK_ROLE_BLK_ID is not null
    group by CUST_BLK_ROLE_BLK_ID
    having count(*) > 1

) validation_errors


