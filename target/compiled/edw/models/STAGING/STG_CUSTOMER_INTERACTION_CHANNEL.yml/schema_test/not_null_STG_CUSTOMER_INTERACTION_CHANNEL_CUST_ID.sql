
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL
where CUST_ID is null


