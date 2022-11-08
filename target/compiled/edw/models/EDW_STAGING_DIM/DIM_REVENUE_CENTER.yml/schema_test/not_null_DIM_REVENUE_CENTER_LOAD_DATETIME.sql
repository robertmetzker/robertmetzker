
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_REVENUE_CENTER
where LOAD_DATETIME is null


