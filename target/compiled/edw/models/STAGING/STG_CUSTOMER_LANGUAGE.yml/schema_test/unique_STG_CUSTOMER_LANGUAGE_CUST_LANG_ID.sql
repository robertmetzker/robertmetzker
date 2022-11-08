
    
    



select count(*) as validation_errors
from (

    select
        CUST_LANG_ID

    from STAGING.STG_CUSTOMER_LANGUAGE
    where CUST_LANG_ID is not null
    group by CUST_LANG_ID
    having count(*) > 1

) validation_errors


