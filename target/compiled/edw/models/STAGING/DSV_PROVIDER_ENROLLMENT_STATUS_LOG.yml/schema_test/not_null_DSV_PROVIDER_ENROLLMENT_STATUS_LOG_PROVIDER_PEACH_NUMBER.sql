
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER_ENROLLMENT_STATUS_LOG
where PROVIDER_PEACH_NUMBER is null


