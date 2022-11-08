
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE
where LOAD_DATETIME is null


