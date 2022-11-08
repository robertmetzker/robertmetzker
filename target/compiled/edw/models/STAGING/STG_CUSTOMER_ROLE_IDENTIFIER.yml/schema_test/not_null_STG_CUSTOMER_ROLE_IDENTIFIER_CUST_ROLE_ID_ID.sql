
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ROLE_IDENTIFIER
where CUST_ROLE_ID_ID is null


