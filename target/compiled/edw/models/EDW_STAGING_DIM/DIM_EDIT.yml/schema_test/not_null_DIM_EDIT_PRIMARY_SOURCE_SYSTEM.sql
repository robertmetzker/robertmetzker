
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EDIT
where PRIMARY_SOURCE_SYSTEM is null


