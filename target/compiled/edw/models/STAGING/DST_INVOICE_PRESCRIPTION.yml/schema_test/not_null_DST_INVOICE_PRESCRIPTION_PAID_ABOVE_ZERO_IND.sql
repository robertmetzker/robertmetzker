
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where PAID_ABOVE_ZERO_IND is null


