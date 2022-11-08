
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_ENROLLMENT_STATUS
where PRVDR_BASE_NMBR is null


