
    
    



select count(*) as validation_errors
from STAGING.DST_PARTICIPATION_TYPE
where UNIQUE_ID_KEY is null


