
    
    



select count(*) as validation_errors
from STAGING.DST_MANUAL_CLASSIFICATION
where UNIQUE_ID_KEY is null


