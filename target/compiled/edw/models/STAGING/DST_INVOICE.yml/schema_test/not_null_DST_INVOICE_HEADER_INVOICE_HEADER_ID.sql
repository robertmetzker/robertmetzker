
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where HEADER_INVOICE_HEADER_ID is null


