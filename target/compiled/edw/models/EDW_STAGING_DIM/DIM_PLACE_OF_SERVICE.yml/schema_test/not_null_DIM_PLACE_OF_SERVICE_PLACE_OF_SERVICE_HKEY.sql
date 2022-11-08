
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PLACE_OF_SERVICE
where PLACE_OF_SERVICE_HKEY is null


