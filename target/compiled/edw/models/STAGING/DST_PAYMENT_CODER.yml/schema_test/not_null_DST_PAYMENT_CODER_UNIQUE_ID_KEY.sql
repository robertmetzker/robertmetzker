
    
    



select count(*) as validation_errors
from STAGING.DST_PAYMENT_CODER
where UNIQUE_ID_KEY is null


