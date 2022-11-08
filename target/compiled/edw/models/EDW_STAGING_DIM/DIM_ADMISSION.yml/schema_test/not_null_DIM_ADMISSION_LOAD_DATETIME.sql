
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ADMISSION
where LOAD_DATETIME is null


