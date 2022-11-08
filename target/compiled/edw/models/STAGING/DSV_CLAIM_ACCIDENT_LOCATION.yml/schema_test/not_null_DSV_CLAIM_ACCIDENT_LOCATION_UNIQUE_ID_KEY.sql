
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_ACCIDENT_LOCATION
where UNIQUE_ID_KEY is null


