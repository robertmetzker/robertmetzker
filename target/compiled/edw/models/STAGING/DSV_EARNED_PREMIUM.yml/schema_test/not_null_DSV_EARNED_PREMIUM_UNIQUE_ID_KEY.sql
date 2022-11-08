
    
    



select count(*) as validation_errors
from STAGING.DSV_EARNED_PREMIUM
where UNIQUE_ID_KEY is null


