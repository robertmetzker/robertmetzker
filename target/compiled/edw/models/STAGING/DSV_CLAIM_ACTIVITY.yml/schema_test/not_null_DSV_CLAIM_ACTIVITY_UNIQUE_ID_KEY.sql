
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_ACTIVITY
where UNIQUE_ID_KEY is null


