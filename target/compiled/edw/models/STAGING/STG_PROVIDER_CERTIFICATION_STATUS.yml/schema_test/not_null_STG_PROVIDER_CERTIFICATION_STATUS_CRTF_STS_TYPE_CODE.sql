
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_CERTIFICATION_STATUS
where CRTF_STS_TYPE_CODE is null


