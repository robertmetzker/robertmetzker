
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_TIME
where LOAD_DATETIME is null


