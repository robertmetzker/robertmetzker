
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ORGANIZATIONAL_UNIT
where PRIMARY_SOURCE_SYSTEM is null


