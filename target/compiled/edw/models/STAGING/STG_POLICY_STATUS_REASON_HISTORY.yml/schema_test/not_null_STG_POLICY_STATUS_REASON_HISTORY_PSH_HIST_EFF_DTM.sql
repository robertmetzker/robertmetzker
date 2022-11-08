
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_STATUS_REASON_HISTORY
where PSH_HIST_EFF_DTM is null


