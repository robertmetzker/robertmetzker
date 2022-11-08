
    
    



select count(*) as validation_errors
from STAGING.DST_PAYEE_NAME
where UNIQUE_ID_KEY is null


