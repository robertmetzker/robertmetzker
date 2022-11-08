
    
    



select count(*) as validation_errors
from STAGING.DSV_ACTIVITY_DETAIL
where UNIQUE_ID_KEY is null


