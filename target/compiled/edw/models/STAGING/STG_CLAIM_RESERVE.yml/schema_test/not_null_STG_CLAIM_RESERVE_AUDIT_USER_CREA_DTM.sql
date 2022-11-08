
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_RESERVE
where AUDIT_USER_CREA_DTM is null


