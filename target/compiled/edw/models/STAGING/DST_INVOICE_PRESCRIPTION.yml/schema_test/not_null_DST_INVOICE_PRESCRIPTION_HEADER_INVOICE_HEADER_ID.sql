
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where HEADER_INVOICE_HEADER_ID is null


