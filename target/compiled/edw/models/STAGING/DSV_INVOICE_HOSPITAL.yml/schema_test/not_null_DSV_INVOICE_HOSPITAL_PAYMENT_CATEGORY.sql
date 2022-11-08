
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_HOSPITAL
where PAYMENT_CATEGORY is null


