
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where SEQUENCE_EXTENSION is null


