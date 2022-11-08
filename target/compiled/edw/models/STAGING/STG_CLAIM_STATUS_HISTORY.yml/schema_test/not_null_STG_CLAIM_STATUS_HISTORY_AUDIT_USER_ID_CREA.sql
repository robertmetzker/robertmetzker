
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_STATUS_HISTORY
where AUDIT_USER_ID_CREA is null


