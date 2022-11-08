
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INVOICE_STATUS
where INVOICE_STATUS_HKEY is null


