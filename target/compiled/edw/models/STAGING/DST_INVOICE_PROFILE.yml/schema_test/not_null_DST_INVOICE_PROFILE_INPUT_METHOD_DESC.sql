
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PROFILE
where INPUT_METHOD_DESC is null


