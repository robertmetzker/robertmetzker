
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PROVIDER_CERTIFICATION_STATUS
where BWC_CERTIFICATION_STATUS_HKEY is null


