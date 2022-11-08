
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_USER
where USER_HKEY is null


