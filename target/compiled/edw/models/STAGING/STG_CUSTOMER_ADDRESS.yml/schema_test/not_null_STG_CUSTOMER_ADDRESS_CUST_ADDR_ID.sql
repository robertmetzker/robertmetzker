
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ADDRESS
where CUST_ADDR_ID is null


