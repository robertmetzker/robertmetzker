
    
    



select count(*) as validation_errors
from STAGING.STG_BUSINESS_CUSTOMER
where AUDIT_USER_ID_CREA is null

