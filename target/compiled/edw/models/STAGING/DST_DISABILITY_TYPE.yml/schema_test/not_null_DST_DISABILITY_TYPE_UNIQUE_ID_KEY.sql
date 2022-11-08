
    
    



select count(*) as validation_errors
from STAGING.DST_DISABILITY_TYPE
where UNIQUE_ID_KEY is null


