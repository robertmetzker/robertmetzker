
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_MANUAL_CLASSIFICATION
where LOAD_DATETIME is null


