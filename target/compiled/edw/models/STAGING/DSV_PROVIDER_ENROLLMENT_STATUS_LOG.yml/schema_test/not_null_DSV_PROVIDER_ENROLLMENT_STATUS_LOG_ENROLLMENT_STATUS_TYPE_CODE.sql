
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER_ENROLLMENT_STATUS_LOG
where ENROLLMENT_STATUS_TYPE_CODE is null


