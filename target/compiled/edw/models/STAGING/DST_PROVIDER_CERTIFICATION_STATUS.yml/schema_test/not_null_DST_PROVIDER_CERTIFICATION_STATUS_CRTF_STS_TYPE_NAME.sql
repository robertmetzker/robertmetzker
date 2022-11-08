
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER_CERTIFICATION_STATUS
where CRTF_STS_TYPE_NAME is null


