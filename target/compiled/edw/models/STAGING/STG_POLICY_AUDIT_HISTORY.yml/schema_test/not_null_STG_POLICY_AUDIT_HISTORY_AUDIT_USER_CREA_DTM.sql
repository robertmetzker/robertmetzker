
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_AUDIT_HISTORY
where AUDIT_USER_CREA_DTM is null

