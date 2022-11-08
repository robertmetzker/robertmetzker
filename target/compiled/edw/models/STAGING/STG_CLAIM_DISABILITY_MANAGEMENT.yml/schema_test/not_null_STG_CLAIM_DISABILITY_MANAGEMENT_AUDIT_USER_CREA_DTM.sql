
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_DISABILITY_MANAGEMENT
where AUDIT_USER_CREA_DTM is null


