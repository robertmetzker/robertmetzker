
    
    



select count(*) as validation_errors
from STAGING.DSV_HEALTHCARE_AUTHORIZATION_STATUS
where UNIQUE_ID_KEY is null


