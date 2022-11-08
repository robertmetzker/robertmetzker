
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_ICD_STATUS_HISTORY
where HIST_EFF_DTM is null


