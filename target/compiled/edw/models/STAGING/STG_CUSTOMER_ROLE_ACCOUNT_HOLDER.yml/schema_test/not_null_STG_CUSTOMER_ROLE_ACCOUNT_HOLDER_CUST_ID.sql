
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER
where CUST_ID is null


