
    
    



select count(*) as validation_errors
from (

    select
        CUST_ROLE_ID_ID

    from STAGING.STG_CUSTOMER_ROLE_IDENTIFIER
    where CUST_ROLE_ID_ID is not null
    group by CUST_ROLE_ID_ID
    having count(*) > 1

) validation_errors


