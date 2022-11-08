
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_ACTIVITY
where ACTIVITY_DETAIL_ID is null


