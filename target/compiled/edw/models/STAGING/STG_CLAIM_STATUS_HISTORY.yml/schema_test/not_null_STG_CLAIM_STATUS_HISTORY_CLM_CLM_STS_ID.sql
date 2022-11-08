
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_STATUS_HISTORY
where CLM_CLM_STS_ID is null


