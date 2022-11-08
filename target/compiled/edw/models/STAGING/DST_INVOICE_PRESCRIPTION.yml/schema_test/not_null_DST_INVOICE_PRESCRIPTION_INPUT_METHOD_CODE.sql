
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where INPUT_METHOD_CODE is null


