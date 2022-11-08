
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM
where UNIQUE_ID_KEY is null


