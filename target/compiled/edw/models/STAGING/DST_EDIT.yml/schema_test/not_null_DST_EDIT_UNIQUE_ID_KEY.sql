
    
    



select count(*) as validation_errors
from STAGING.DST_EDIT
where UNIQUE_ID_KEY is null


