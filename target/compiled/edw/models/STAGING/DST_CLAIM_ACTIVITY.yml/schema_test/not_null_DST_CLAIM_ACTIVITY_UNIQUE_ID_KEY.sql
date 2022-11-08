
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_ACTIVITY
where UNIQUE_ID_KEY is null


