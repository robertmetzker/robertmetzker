
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_MANUAL_CLASSIFICATION
where MANUAL_CLASS_HKEY is null


