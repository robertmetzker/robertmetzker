
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where LINE_VERSION_NUMBER is null


