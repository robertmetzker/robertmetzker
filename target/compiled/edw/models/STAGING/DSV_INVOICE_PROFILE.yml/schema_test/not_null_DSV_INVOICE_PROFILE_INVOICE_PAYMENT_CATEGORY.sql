
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where INVOICE_PAYMENT_CATEGORY is null


