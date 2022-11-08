
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_AUDIT_STATUS_HISTORY
where AUDIT_USER_ID_CREA is null


