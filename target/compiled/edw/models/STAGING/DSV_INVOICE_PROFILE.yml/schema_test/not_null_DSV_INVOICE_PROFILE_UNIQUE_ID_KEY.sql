
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where UNIQUE_ID_KEY is null


