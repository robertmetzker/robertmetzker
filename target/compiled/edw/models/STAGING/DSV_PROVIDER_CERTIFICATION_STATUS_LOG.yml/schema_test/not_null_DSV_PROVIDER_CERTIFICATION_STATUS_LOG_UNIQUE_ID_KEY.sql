
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER_CERTIFICATION_STATUS_LOG
where UNIQUE_ID_KEY is null


