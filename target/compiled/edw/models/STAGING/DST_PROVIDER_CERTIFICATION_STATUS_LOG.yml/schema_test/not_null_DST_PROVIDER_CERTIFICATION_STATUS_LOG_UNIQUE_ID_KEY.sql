
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER_CERTIFICATION_STATUS_LOG
where UNIQUE_ID_KEY is null


