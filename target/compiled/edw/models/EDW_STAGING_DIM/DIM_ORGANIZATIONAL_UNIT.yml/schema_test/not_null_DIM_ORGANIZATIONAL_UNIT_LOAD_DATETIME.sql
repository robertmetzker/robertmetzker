
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ORGANIZATIONAL_UNIT
where LOAD_DATETIME is null


