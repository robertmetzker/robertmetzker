
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where INVOICE_NUMBER is null


