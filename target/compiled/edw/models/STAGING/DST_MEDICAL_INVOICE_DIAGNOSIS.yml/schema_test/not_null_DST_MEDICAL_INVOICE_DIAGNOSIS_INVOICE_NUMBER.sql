
    
    



select count(*) as validation_errors
from STAGING.DST_MEDICAL_INVOICE_DIAGNOSIS
where INVOICE_NUMBER is null


