
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE
where PRIMARY_SOURCE_SYSTEM is null


