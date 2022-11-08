
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_TIME
where TIME_KEY is null


