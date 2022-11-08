
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER
where AUDIT_USER_CREA_DTM is null


