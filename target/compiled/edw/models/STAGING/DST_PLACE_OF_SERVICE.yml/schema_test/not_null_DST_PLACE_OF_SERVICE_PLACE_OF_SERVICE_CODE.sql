
    
    



select count(*) as validation_errors
from STAGING.DST_PLACE_OF_SERVICE
where PLACE_OF_SERVICE_CODE is null


