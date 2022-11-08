
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_DOCUMENTS
where AUDIT_USER_CREA_DT is null


