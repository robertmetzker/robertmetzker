
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_STATUS
where INVOICE_STATUS_CODE is null


