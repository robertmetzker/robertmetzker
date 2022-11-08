
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER_ENROLLMENT_STATUS
where ENRL_STS_TYPE_CODE is null


