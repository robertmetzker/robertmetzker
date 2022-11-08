
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where PAYMENT_CATEGORY is null


