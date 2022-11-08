
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PROFILE
where INVOICE_TYPE is null


