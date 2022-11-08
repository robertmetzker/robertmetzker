
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE
where UNIQUE_ID_KEY is null


