
    
    



select count(*) as validation_errors
from STAGING.DST_ACTIVITY
where UNIQUE_ID_KEY is null

