
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER_ENROLLMENT_STATUS_LOG
where PRVDR_BASE_NMBR is null


