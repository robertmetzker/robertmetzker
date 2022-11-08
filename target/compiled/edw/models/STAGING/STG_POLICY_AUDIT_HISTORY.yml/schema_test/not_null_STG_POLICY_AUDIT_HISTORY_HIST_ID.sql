
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_AUDIT_HISTORY
where HIST_ID is null


