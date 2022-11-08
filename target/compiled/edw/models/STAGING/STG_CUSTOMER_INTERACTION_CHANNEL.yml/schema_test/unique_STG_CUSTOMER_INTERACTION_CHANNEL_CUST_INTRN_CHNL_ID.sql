
    
    



select count(*) as validation_errors
from (

    select
        CUST_INTRN_CHNL_ID

    from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL
    where CUST_INTRN_CHNL_ID is not null
    group by CUST_INTRN_CHNL_ID
    having count(*) > 1

) validation_errors


