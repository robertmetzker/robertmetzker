
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INJURED_WORKER
where INJURED_WORKER_HKEY is null


