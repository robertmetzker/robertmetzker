
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_HOSPITAL
where INPUT_METHOD_CODE is null


