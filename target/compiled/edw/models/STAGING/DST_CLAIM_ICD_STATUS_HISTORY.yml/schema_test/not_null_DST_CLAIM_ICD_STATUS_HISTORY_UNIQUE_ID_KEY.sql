
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_ICD_STATUS_HISTORY
where UNIQUE_ID_KEY is null


