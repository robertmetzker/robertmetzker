
    
    



select count(*) as validation_errors
from STAGING.DSV_INVOICE_PROFILE
where PAID_ABOVE_ZERO_IND is null


