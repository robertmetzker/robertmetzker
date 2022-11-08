
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_STATUS
where MEDICAL_INVOICE_STATUS_CODE is null


