
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_ACCIDENT_LOCATION
where UNIQUE_ID_KEY is null


