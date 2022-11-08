
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_PAYMENT
where AUDIT_USER_ID_CREA is null


