
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_CHILD_SUPPORT
where AUDIT_USER_ID_CREA is null


