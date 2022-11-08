
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_ACTIVITY
where ACTIVITY_ID is null


