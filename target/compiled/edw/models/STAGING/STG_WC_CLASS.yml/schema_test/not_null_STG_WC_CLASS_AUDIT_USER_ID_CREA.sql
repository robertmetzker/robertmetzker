
    
    



select count(*) as validation_errors
from STAGING.STG_WC_CLASS
where AUDIT_USER_ID_CREA is null


