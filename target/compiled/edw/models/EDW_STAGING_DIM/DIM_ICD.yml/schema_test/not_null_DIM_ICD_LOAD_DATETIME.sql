
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ICD
where LOAD_DATETIME is null


