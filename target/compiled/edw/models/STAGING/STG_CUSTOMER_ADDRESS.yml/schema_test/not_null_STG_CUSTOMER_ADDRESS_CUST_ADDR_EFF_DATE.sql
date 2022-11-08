
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ADDRESS
where CUST_ADDR_EFF_DATE is null


