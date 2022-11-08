
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_STATUS
where CURRENT_IND is null


