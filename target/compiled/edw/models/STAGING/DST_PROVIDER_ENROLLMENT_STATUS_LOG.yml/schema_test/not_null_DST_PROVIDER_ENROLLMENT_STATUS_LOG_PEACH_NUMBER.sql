
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER_ENROLLMENT_STATUS_LOG
where PEACH_NUMBER is null


