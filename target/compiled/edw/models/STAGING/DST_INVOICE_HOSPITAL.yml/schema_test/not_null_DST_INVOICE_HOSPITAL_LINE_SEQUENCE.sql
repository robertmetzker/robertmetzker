
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_HOSPITAL
where LINE_SEQUENCE is null


