
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DISABILITY_TYPE
where DISABILITY_TYPE_HKEY is null


