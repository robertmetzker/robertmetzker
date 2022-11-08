
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_ICD_STATUS_HISTORY
where UNIQUE_ID_KEY is null


