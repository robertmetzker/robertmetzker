
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_INJURY_HISTORY
where UNIQUE_ID_KEY is null


