
    
    



select count(*) as validation_errors
from STAGING.DST_WRITTEN_PREMIUM_ELEMENT
where UNIQUE_ID_KEY is null


