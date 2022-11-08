
    
    



select count(*) as validation_errors
from STAGING.DSV_ACTIVITY
where UNIQUE_ID_KEY is null


