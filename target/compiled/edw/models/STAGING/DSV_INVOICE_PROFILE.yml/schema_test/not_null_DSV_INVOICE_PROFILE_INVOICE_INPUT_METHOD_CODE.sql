
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where INVOICE_INPUT_METHOD_CODE is null


