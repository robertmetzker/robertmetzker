
    
    



select count(*) as validation_errors
from STAGING.DST_MEDICAL_INVOICE_PROCEDURE
where INVOICE_NUMBER is null


