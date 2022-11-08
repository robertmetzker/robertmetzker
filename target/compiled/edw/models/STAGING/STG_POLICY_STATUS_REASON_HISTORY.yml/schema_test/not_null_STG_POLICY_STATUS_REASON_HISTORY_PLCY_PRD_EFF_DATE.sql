
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_STATUS_REASON_HISTORY
where PLCY_PRD_EFF_DATE is null


