
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PROVIDER_ENROLLMENT_STATUS
where ENROLLMENT_STATUS_HKEY is null


