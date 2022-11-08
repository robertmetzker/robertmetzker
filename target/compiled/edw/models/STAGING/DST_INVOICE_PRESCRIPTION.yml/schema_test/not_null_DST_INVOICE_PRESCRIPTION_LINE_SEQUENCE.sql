
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where LINE_SEQUENCE is null


