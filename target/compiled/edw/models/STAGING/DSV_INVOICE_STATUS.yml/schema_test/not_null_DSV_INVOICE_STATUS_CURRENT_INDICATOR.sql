
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_STATUS
where CURRENT_INDICATOR is null


