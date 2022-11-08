
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PARTICIPATION_TYPE
where PRIMARY_SOURCE_SYSTEM is null


