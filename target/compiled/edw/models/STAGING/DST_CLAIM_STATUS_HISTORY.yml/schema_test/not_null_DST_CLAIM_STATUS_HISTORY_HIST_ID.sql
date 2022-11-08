
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_STATUS_HISTORY
where HIST_ID is null


