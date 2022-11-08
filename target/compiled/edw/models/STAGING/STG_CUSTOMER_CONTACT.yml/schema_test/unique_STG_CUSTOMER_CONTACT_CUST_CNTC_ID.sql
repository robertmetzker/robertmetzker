
    
    



select count(*) as validation_errors
from (

    select
        CUST_CNTC_ID

    from STAGING.STG_CUSTOMER_CONTACT
    where CUST_CNTC_ID is not null
    group by CUST_CNTC_ID
    having count(*) > 1

) validation_errors


