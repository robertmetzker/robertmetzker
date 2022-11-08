
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_ICD_STATUS_HISTORY
where ICD_ID is null


