
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ADMISSION
where ADMISSION_HKEY is null


