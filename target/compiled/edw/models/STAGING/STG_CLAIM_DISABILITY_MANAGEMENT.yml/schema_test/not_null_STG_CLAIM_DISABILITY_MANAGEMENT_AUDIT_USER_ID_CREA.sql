
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_DISABILITY_MANAGEMENT
where AUDIT_USER_ID_CREA is null


