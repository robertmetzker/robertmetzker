
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where INVOICE_NUMBER is null


