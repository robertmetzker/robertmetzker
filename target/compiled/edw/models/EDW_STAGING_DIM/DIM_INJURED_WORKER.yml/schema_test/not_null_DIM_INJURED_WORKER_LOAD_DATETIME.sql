
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INJURED_WORKER
where LOAD_DATETIME is null


