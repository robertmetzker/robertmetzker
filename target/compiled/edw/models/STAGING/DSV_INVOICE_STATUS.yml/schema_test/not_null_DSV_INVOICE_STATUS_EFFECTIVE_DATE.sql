
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_STATUS
where EFFECTIVE_DATE is null


