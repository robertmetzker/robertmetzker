
    
    



select count(*) as validation_errors
from STAGING.DST_ADMISSION
where UNIQUE_ID_KEY is null


