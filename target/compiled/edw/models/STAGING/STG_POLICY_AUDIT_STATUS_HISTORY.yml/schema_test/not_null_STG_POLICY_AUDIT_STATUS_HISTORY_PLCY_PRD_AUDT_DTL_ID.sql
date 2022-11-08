
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_AUDIT_STATUS_HISTORY
where PLCY_PRD_AUDT_DTL_ID is null


