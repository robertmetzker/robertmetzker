
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PROFILE
where PAYMENT_CATEGORY is null


