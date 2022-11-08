
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_BLOCK
where CUST_BLK_ROLE_BLK_ID is null


