
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where MEDICAL_INVOICE_TYPE_CODE is null


