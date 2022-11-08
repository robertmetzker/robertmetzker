
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ADMISSION
where PRIMARY_SOURCE_SYSTEM is null


