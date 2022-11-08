
    
    



select count(*) as validation_errors
from STAGING.STG_TAX_IDENTIFIER
where AUDIT_USER_ID_CREA is null


