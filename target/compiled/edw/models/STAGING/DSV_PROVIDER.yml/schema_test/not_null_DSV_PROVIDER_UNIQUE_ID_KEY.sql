
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER
where UNIQUE_ID_KEY is null


