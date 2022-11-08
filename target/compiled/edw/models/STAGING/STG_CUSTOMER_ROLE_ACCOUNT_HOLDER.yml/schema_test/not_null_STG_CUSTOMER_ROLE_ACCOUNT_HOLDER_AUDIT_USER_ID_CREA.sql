
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER
where AUDIT_USER_ID_CREA is null


