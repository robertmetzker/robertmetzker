
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PRESCRIPTION
where UNIQUE_ID_KEY is null


