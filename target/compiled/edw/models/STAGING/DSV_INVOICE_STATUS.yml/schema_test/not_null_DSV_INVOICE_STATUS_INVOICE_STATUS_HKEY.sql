
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_STATUS
where INVOICE_STATUS_HKEY is null


