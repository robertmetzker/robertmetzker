
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS
where HEALTHCARE_AUTHORIZATION_STATUS_HKEY is null


