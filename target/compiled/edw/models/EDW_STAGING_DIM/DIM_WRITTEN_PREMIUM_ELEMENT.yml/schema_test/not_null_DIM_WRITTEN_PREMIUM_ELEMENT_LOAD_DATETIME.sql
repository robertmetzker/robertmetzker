
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_WRITTEN_PREMIUM_ELEMENT
where LOAD_DATETIME is null


