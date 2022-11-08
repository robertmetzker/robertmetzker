
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_RATING_ELEMENT
where PRIMARY_SOURCE_SYSTEM is null


