
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where SEQUENCE_EXTENSION is null


