
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_CONTACT_DETAIL
where AUDIT_USER_CREA_DTM is null


