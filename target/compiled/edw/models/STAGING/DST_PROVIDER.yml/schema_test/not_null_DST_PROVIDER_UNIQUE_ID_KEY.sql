
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER
where UNIQUE_ID_KEY is null


