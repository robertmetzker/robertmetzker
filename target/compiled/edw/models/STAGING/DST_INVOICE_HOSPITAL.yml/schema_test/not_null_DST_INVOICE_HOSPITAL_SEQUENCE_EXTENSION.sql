
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where SEQUENCE_EXTENSION is null


