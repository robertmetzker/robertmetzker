
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where PAID_ABOVE_ZERO_IND is null


