
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER
where ROLE_ID is null


