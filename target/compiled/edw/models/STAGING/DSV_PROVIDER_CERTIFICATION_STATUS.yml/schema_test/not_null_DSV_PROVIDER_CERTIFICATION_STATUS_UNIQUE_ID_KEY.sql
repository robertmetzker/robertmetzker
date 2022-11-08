
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER_CERTIFICATION_STATUS
where UNIQUE_ID_KEY is null


