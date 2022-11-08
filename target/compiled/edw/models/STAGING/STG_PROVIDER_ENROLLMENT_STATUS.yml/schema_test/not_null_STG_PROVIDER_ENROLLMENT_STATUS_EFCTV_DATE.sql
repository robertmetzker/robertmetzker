
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_ENROLLMENT_STATUS
where EFCTV_DATE is null


