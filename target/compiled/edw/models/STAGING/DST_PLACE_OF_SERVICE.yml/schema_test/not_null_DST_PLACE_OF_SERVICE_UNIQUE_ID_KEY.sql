
    
    



select count(*) as validation_errors
from STAGING.DST_PLACE_OF_SERVICE
where UNIQUE_ID_KEY is null


