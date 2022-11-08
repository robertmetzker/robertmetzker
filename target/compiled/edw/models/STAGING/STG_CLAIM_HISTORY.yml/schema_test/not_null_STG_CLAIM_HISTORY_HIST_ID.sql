
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_HISTORY
where HIST_ID is null


