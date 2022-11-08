
    
    



select count(*) as validation_errors
from STAGING.STG_BUSINESS_CUSTOMER
where AUDIT_USER_CREA_DTM is null


