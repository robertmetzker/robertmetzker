
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ACTIVITY_DETAIL
where ACTIVITY_DETAIL_HKEY is null


