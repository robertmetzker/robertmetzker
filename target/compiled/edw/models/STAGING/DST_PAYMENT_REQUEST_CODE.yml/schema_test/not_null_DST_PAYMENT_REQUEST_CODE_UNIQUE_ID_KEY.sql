
    
    



select count(*) as validation_errors
from STAGING.DST_PAYMENT_REQUEST_CODE
where UNIQUE_ID_KEY is null


