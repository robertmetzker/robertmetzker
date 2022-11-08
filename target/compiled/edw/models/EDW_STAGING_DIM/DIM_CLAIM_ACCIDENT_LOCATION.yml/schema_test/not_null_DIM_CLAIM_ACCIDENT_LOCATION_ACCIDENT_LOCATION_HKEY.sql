
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ACCIDENT_LOCATION
where ACCIDENT_LOCATION_HKEY is null


