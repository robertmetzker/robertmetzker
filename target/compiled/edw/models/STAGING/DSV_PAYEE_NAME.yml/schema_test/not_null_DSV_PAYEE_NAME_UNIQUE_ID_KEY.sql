
    
    



select count(*) as validation_errors
from STAGING.DSV_PAYEE_NAME
where UNIQUE_ID_KEY is null


