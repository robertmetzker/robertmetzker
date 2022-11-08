
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where PAID_ABOVE_ZERO_IND is null


