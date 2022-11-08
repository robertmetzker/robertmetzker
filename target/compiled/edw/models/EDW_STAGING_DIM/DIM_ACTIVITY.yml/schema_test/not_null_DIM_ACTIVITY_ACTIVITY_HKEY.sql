
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ACTIVITY
where ACTIVITY_HKEY is null


