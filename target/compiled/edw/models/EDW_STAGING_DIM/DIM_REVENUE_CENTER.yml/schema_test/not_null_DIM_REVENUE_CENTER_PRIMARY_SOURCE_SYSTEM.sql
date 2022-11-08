
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_REVENUE_CENTER
where PRIMARY_SOURCE_SYSTEM is null


