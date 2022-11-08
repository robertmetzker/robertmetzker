
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_RATING_ELEMENT
where LOAD_DATETIME is null


