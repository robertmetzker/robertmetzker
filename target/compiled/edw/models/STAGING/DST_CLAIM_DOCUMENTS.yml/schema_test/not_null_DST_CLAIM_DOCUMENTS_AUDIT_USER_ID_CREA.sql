
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_DOCUMENTS
where AUDIT_USER_ID_CREA is null


