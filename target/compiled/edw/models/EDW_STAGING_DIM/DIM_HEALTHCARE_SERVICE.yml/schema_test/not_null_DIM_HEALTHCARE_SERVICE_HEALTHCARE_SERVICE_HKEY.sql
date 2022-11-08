
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE
where HEALTHCARE_SERVICE_HKEY is null


