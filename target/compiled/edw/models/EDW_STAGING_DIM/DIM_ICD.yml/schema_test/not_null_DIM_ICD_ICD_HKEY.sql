
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_ICD
where ICD_HKEY is null

