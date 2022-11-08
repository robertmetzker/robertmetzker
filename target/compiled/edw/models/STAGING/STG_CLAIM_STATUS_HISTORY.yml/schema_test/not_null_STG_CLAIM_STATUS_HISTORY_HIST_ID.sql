
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_STATUS_HISTORY
where HIST_ID is null


