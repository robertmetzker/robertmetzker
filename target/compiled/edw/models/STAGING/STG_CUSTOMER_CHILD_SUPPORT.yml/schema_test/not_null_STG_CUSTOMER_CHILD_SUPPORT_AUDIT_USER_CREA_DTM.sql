
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_CHILD_SUPPORT
where AUDIT_USER_CREA_DTM is null


