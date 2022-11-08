
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where INVOICE_NUMBER is null


