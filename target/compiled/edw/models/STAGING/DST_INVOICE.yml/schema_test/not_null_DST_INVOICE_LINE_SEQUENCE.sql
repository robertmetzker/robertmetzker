
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where LINE_SEQUENCE is null


