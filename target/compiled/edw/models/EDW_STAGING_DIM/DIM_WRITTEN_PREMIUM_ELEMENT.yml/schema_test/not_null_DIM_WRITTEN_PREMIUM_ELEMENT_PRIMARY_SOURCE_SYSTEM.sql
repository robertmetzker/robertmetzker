
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_WRITTEN_PREMIUM_ELEMENT
where PRIMARY_SOURCE_SYSTEM is null


