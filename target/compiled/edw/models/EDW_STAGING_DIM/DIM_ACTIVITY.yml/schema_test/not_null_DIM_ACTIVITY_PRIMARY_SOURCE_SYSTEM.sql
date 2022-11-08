
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ACTIVITY
where PRIMARY_SOURCE_SYSTEM is null


