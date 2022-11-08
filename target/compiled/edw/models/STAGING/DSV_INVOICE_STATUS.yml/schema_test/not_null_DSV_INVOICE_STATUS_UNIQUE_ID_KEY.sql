
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_STATUS
where UNIQUE_ID_KEY is null


