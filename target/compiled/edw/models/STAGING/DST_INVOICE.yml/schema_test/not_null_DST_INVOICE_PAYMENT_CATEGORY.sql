
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where PAYMENT_CATEGORY is null


