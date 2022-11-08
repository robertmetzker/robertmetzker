
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE
where PRESENT_ON_ADMISSION_HKEY is null


