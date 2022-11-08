
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EDIT_EOB_ENTRY
where PRIMARY_SOURCE_SYSTEM is null


