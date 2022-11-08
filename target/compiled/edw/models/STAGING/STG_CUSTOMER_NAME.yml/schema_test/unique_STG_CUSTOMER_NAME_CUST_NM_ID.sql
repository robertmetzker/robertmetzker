
    
    



select count(*) as validation_errors
from (

    select
        CUST_NM_ID

    from STAGING.STG_CUSTOMER_NAME
    where CUST_NM_ID is not null
    group by CUST_NM_ID
    having count(*) > 1

) validation_errors


