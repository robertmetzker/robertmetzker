
    
    



select count(*) as validation_errors
from (

    select
        CUST_CHLD_SUPT_ID

    from STAGING.STG_CUSTOMER_CHILD_SUPPORT
    where CUST_CHLD_SUPT_ID is not null
    group by CUST_CHLD_SUPT_ID
    having count(*) > 1

) validation_errors


