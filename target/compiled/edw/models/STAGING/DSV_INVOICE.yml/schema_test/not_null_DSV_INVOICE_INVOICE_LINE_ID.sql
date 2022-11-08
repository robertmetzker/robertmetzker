
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE
where INVOICE_LINE_ID is null


