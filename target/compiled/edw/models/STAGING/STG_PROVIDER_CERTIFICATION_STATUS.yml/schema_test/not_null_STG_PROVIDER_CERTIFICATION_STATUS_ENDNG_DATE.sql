
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_CERTIFICATION_STATUS
where ENDNG_DATE is null


