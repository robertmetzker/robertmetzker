
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_ENROLLMENT_STATUS
where ENDNG_DATE is null


